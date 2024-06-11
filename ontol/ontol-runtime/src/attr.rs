use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::{
    sequence::Sequence,
    tuple::{EndoTuple, EndoTupleElements},
    value::{Value, ValueDebug},
};

/// ONTOL attributes
///
/// Attributes are part of other values. Attributes can be single or multi-valued.
///
/// The variants of this enum describe how attributes may be quantified.
#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub enum Attr {
    /// The attribute has one value
    Unit(Value),
    /// The attribute is horizontally multivalued,
    /// the size is known at (ontol-)compile time.
    ///
    /// The tuple represents siblings in the hyper-edge this attribute is derived from.
    Tuple(Box<EndoTuple<Value>>),
    /// Column-oriented matrix, i.e. tuples first, then sequences within those tuples
    Matrix(AttrMatrix),
}

impl Attr {
    pub fn as_ref(&self) -> AttrRef {
        match self {
            Attr::Unit(v) => AttrRef::Unit(v),
            Attr::Tuple(t) => AttrRef::Tuple(AttrTupleRef::Tuple(&t.elements)),
            Attr::Matrix(m) => AttrRef::Matrix(&m.columns),
        }
    }

    pub fn as_unit(&self) -> Option<&Value> {
        match self {
            Self::Unit(v) => Some(v),
            Self::Tuple(t) => {
                if t.elements.len() == 1 {
                    t.elements.get(0)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_matrix(&self) -> Option<&AttrMatrix> {
        match self {
            Self::Matrix(m) => Some(m),
            _ => None,
        }
    }

    pub fn into_unit(self) -> Option<Value> {
        match self {
            Self::Unit(v) => Some(v),
            Self::Tuple(t) => {
                if t.elements.len() == 1 {
                    t.elements.into_iter().next()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn into_tuple(self) -> Option<EndoTupleElements<Value>> {
        match self {
            Self::Unit(v) => Some(smallvec![v]),
            Self::Tuple(t) => Some(t.elements),
            Self::Matrix(_) => None,
        }
    }

    pub fn unwrap_unit(self) -> Value {
        match self {
            Self::Unit(value) => value,
            Self::Tuple(tuple) => {
                if tuple.elements.len() != 1 {
                    panic!("not a singleton tuple")
                }
                tuple.elements.into_iter().next().unwrap()
            }
            Self::Matrix(_) => {
                panic!("matrix is not a unit")
            }
        }
    }

    // Hack for now
    pub fn unit_or_tuple(value: Value, rel_params: Value) -> Self {
        if rel_params.is_unit() {
            Self::Unit(value)
        } else {
            debug!("making a tuple for rel_params: {}", ValueDebug(&rel_params));
            Self::Tuple(Box::new(EndoTuple {
                elements: smallvec![value, rel_params],
            }))
        }
    }
}

/// A column-first matrix of attributes.
#[derive(Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct AttrMatrix {
    pub columns: EndoTupleElements<Sequence<Value>>,
}

impl AttrMatrix {
    /// Get an AttrRef at specific coordinate in the matrix
    pub fn get_ref(&self, row_index: usize, column_index: usize) -> Option<AttrRef> {
        let column = self.columns.get(column_index)?;

        let value = column.elements().get(row_index)?;

        Some(AttrRef::Unit(value))
    }

    pub fn row_count(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].elements.len()
        }
    }

    pub fn rows(&self) -> AttrMatrixRows {
        let columns = self
            .columns
            .iter()
            .map(|column| column.elements.iter())
            .collect();

        AttrMatrixRows { columns }
    }

    pub fn into_rows(self) -> AttrMatrixIntoRows {
        let columns = self
            .columns
            .into_iter()
            .map(|column| column.elements.into_iter())
            .collect();

        AttrMatrixIntoRows { columns }
    }

    /// Get a full row as a tuple attribute
    pub fn get_row_cloned(&self, row_index: usize) -> Option<Attr> {
        let mut elements: SmallVec<Value, 1> = smallvec![];

        for e in &self.columns {
            elements.push(e.elements.get(row_index)?.clone());
        }

        Some(Attr::Tuple(Box::new(EndoTuple { elements })))
    }
}

pub struct AttrMatrixRows<'a> {
    columns: EndoTupleElements<core::slice::Iter<'a, Value>>,
}

impl<'a> Iterator for AttrMatrixRows<'a> {
    type Item = EndoTuple<&'a Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next_tuple = SmallVec::<&'a Value, 1>::with_capacity(self.columns.len());

        for column in self.columns.iter_mut() {
            let next = column.next()?;

            next_tuple.push(next);
        }

        Some(EndoTuple {
            elements: next_tuple,
        })
    }
}

pub struct AttrMatrixIntoRows {
    columns: EndoTupleElements<thin_vec::IntoIter<Value>>,
}

impl Iterator for AttrMatrixIntoRows {
    type Item = EndoTuple<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next_tuple = SmallVec::<Value, 1>::with_capacity(self.columns.len());

        for column in self.columns.iter_mut() {
            let next = column.next()?;

            next_tuple.push(next);
        }

        Some(EndoTuple {
            elements: next_tuple,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AttrRef<'v> {
    /// One value
    Unit(&'v Value),
    /// A tuple of values
    Tuple(AttrTupleRef<'v>),
    /// A whole matrix
    Matrix(&'v [Sequence<Value>]),
}

impl<'v> AttrRef<'v> {
    #[inline]
    pub fn coerce_to_unit(self) -> Self {
        match self {
            Self::Unit(v) => Self::Unit(v),
            Self::Tuple(t) => {
                if t.arity() == 1 {
                    Self::Unit(t.get(0).unwrap())
                } else {
                    self
                }
            }
            Self::Matrix(m) => Self::Matrix(m),
        }
    }

    pub fn first_unit(self) -> Option<&'v Value> {
        match self {
            Self::Unit(v) => Some(v),
            Self::Tuple(t) => t.get(0),
            Self::Matrix(..) => None,
        }
    }

    pub fn as_unit(&self) -> Option<&Value> {
        match self {
            Self::Unit(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AttrTupleRef<'v> {
    Tuple(&'v [Value]),
    /// Tuple borrowed from a Matrix
    Row(&'v [&'v Value]),
}

impl<'v> AttrTupleRef<'v> {
    pub fn arity(self) -> usize {
        match self {
            Self::Tuple(t) => t.len(),
            Self::Row(r) => r.len(),
        }
    }

    pub fn get(self, index: usize) -> Option<&'v Value> {
        match self {
            Self::Tuple(t) => t.get(index),
            Self::Row(r) => r.get(index).map(|v| *v),
        }
    }
}

impl From<Value> for Attr {
    fn from(value: Value) -> Self {
        Self::Unit(value)
    }
}

impl From<EndoTuple<Value>> for Attr {
    fn from(value: EndoTuple<Value>) -> Self {
        Self::Tuple(Box::new(value))
    }
}
