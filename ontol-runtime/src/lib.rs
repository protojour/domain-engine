pub mod env;
pub mod serde;
pub mod value;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct PropertyId(pub u32);

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct PackageId(pub u32);
