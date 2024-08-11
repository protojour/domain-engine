use std::fmt::Debug;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Buf;
use tokio_postgres::Row;

use crate::{
    pg_model::{PgRegKey, PgType},
    sql_value::{CodecResult, Layout, SqlVal},
};

pub struct RowDecodeIterator<'b> {
    row: &'b Row,
    layout: &'b [Layout],
    index: usize,
}

impl<'b> RowDecodeIterator<'b> {
    pub fn new(row: &'b Row, layout: &'b [Layout]) -> Self {
        Self {
            row,
            layout,
            index: 0,
        }
    }
}

impl<'b> Iterator for RowDecodeIterator<'b> {
    type Item = CodecResult<SqlVal<'b>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row.columns().len() {
            None
        } else {
            let index = self.index;
            self.index += 1;
            let buf = self.row.col_buffer(index);

            Some(SqlVal::decode(buf, &self.layout[index]))
        }
    }
}

pub struct SqlRecord<'b> {
    field_count: i32,
    buf: &'b [u8],
    field_layout: &'b [Layout],
}

impl<'b> SqlRecord<'b> {
    /// It was hard to find the binary documentation for composite/record types.
    /// Used this as a reference: https://github.com/jackc/pgtype/blob/a4d4bbf043f7988ea29696a612cf311026fedf92/composite_type.go
    pub(crate) fn from_sql(mut buf: &'b [u8], field_layout: &'b [Layout]) -> CodecResult<Self> {
        let field_count = read_record_field_count(&mut buf)?;
        if field_count as usize != field_layout.len() {
            return Err(
                "field layout length does not correspond to record value field count".into(),
            );
        }

        Ok(SqlRecord {
            field_count,
            buf,
            field_layout,
        })
    }

    pub fn fields(&self) -> RecordFields<'b, std::slice::Iter<'b, Layout>> {
        RecordFields {
            buf: self.buf,
            layout_iter: self.field_layout.iter(),
        }
    }
}

impl<'b> Debug for SqlRecord<'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlRecord")
            .field("field_count", &self.field_count)
            .finish()
    }
}

/// A record where the full layout is not statically known.
/// The only constraint is that the first field must be an Integer (i32).
/// This integer is then used to find the actual type.
#[derive(Clone, Copy)]
pub struct SqlDynRecord<'b> {
    field_count: i32,
    buf: &'b [u8],
}

impl<'b> SqlDynRecord<'b> {
    pub(crate) fn from_sql(mut buf: &'b [u8]) -> CodecResult<Self> {
        let field_count = read_record_field_count(&mut buf)?;
        Ok(Self { field_count, buf })
    }

    pub fn def_key(&self) -> CodecResult<PgRegKey> {
        let mut buf = self.buf;
        let SqlVal::I32(def_key) = decode_record_field(&mut buf, &Layout::Scalar(PgType::Integer))?
        else {
            return Err("sql dyn record must start with an integer".into());
        };
        Ok(def_key)
    }

    /// An iterator over the rest of the fields (initial Integer field included)
    pub fn fields<L>(&self, layout_iter: L) -> RecordFields<'b, L>
    where
        L: Iterator<Item = &'b Layout>,
    {
        RecordFields {
            buf: self.buf,
            layout_iter,
        }
    }
}

impl<'b> Debug for SqlDynRecord<'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlDynRecord")
            .field("field_count", &self.field_count)
            .field("def_key", &self.def_key())
            .finish()
    }
}

pub struct RecordFields<'b, L> {
    buf: &'b [u8],
    layout_iter: L,
}

impl<'b, L> Iterator for RecordFields<'b, L>
where
    L: Iterator<Item = &'b Layout>,
{
    type Item = CodecResult<SqlVal<'b>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }

        let Some(layout) = self.layout_iter.next() else {
            return Some(Err("more record fields than layout fields".into()));
        };

        Some(decode_record_field(&mut self.buf, layout))
    }
}

fn read_record_field_count(buf: &mut &[u8]) -> CodecResult<i32> {
    let field_count = buf.read_i32::<BigEndian>()?;
    if field_count < 0 {
        return Err("invalid field count".into());
    }
    Ok(field_count)
}

fn decode_record_field<'b>(buf: &mut &'b [u8], layout: &'b Layout) -> CodecResult<SqlVal<'b>> {
    let _oid = buf.read_u32::<BigEndian>()?;
    let field_len = buf.read_i32::<BigEndian>()?;

    if field_len > 0 {
        let field_len = field_len as usize;
        let field_buf = &buf[0..field_len];

        buf.advance(field_len);

        SqlVal::decode(Some(field_buf), layout)
    } else {
        Ok(SqlVal::Null)
    }
}
