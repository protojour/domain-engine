use fnv::FnvHashMap;
use ontol_runtime::DefRelTag;

pub type PgSerial = i64;
pub type DomainUid = ulid::Ulid;

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(i32)]
pub enum RegVersion {
    Init = 1,
}

impl RegVersion {
    pub const fn current() -> Self {
        Self::Init
    }
}

impl TryFrom<i32> for RegVersion {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Init),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
pub struct DefUid(pub DomainUid, pub u16);

#[derive(Clone)]
pub struct PgDomain {
    pub key: Option<PgSerial>,
    pub schema_name: Box<str>,
    pub datatables: FnvHashMap<DefUid, PgDataTable>,
}

#[derive(Clone)]
pub struct PgDataTable {
    pub key: PgSerial,
    pub table_name: Box<str>,
    pub data_fields: FnvHashMap<DefRelTag, PgDataField>,
}

#[derive(Clone)]
pub struct PgDataField {
    pub column_name: Box<str>,
}
