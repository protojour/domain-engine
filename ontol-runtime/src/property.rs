//! The ONTOL model of a property

use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};

use crate::{impl_ontol_debug, DefId, PackageId, RelationshipId};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub enum Role {
    Subject,
    Object,
}

impl Role {
    /// Get the role that is opposite to this role
    pub fn opposite(self) -> Self {
        match self {
            Self::Subject => Self::Object,
            Self::Object => Self::Subject,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PropertyId {
    pub role: Role,
    pub relationship_id: RelationshipId,
}

impl Display for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            match self.role {
                Role::Subject => 'S',
                Role::Object => 'O',
            },
            self.relationship_id.0 .0 .0,
            self.relationship_id.0 .1,
        )
    }
}

impl FromStr for PropertyId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iterator = s.split(':');
        let role = match iterator.next().ok_or(())? {
            "S" => Role::Subject,
            "O" => Role::Object,
            _ => Err(())?,
        };
        let package_id = PackageId(iterator.next().ok_or(())?.parse().map_err(|_| ())?);
        let def_idx: u16 = iterator.next().ok_or(())?.parse().map_err(|_| ())?;

        if iterator.next().is_some() {
            return Err(());
        }

        Ok(PropertyId {
            role,
            relationship_id: RelationshipId(DefId(package_id, def_idx)),
        })
    }
}

impl Debug for PropertyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl_ontol_debug!(PropertyId);

impl PropertyId {
    pub const fn subject(relationship_id: RelationshipId) -> Self {
        Self {
            role: Role::Subject,
            relationship_id,
        }
    }

    pub const fn object(relationship_id: RelationshipId) -> Self {
        Self {
            role: Role::Object,
            relationship_id,
        }
    }
}

pub type Cardinality = (PropertyCardinality, ValueCardinality);

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug, OntolDebug,
)]
pub enum PropertyCardinality {
    Optional,
    Mandatory,
}

impl PropertyCardinality {
    pub fn is_mandatory(&self) -> bool {
        matches!(self, Self::Mandatory)
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional)
    }
}

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Debug, OntolDebug,
)]
pub enum ValueCardinality {
    One,
    Many,
}
