use serde::{
    de::{DeserializeSeed, Error, Visitor},
    Deserializer,
};
use smartstring::alias::String;

use crate::{ontology::domain::TypeKind, phf::PhfIndexMap, RelationshipId};

use super::{
    deserialize_struct::StructDeserializer,
    operator::{SerdeOperatorAddr, SerdeProperty, SerdePropertyFlags, SerdeStructFlags},
    processor::{ProcessorMode, ProcessorProfileFlags, SpecialProperty},
};

/// The types of properties the deserializer understands.
pub enum PropKind {
    Property(SerdeProperty),
    RelParams(SerdeOperatorAddr),
    SingletonId(SerdeOperatorAddr),
    OverriddenId(RelationshipId, SerdeOperatorAddr),
    Open(String),
    Ignored,
}

/// A visitor for properties (i.e. _keys, not their values, which are the attributes).
/// It determines the semantics of each property, or whether it's accepted or not.
#[derive(Clone, Copy)]
pub struct PropertyMapVisitor<'d> {
    pub deserializer: &'d StructDeserializer<'d, 'd>,
    pub properties: &'d PhfIndexMap<SerdeProperty>,
}

impl<'a, 'de> DeserializeSeed<'de> for PropertyMapVisitor<'a> {
    type Value = PropKind;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'a, 'de> Visitor<'de> for PropertyMapVisitor<'a> {
    type Value = PropKind;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<PropKind, E> {
        let Some(serde_property) = self.properties.get(v) else {
            return fallback(v, self.deserializer);
        };

        if serde_property
            .flags
            .contains(SerdePropertyFlags::REL_PARAMS)
        {
            return if let Some(addr) = self.deserializer.rel_params_addr {
                Ok(PropKind::RelParams(addr))
            } else {
                Err(Error::custom(format!("`{}` property not accepted here", v)))
            };
        }

        if serde_property
            .filter(
                self.deserializer.processor.mode,
                self.deserializer.processor.ctx.parent_property_id,
                self.deserializer.processor.profile.flags,
            )
            .is_some()
        {
            Ok(PropKind::Property(*serde_property))
        } else if serde_property.is_read_only()
            && !matches!(self.deserializer.processor.mode, ProcessorMode::Read)
        {
            Err(Error::custom(format!("property `{v}` is read-only")))
        } else {
            Err(Error::custom(format!(
                "property `{v}` not available in this context"
            )))
        }
    }
}

#[derive(Clone, Copy)]
pub struct IdSingletonPropVisitor<'d> {
    pub deserializer: &'d StructDeserializer<'d, 'd>,
    pub id_prop_name: &'d str,
    pub id_prop_addr: SerdeOperatorAddr,
}

impl<'a, 'de> DeserializeSeed<'de> for IdSingletonPropVisitor<'a> {
    type Value = PropKind;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'a, 'de> Visitor<'de> for IdSingletonPropVisitor<'a> {
    type Value = PropKind;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<PropKind, E> {
        if v == self.id_prop_name {
            Ok(PropKind::SingletonId(self.id_prop_addr))
        } else if v
            == self
                .deserializer
                .processor
                .ontology
                .ontol_domain_meta()
                .edge_property
        {
            if let Some(addr) = self.deserializer.rel_params_addr {
                Ok(PropKind::RelParams(addr))
            } else {
                Err(Error::custom("`_edge` property not accepted here"))
            }
        } else {
            fallback(v, self.deserializer)
        }
    }
}

fn fallback<E: Error>(v: &str, deserializer: &StructDeserializer) -> Result<PropKind, E> {
    match deserializer
        .processor
        .profile
        .api
        .lookup_special_property(v)
    {
        Some(SpecialProperty::IdOverride) => {
            let type_info = deserializer
                .processor
                .ontology
                .get_type_info(deserializer.type_def_id);
            let TypeKind::Entity(entity_info) = &type_info.kind else {
                return Err(E::custom("not an entity"));
            };

            return Ok(PropKind::OverriddenId(
                entity_info.id_relationship_id,
                entity_info.id_operator_addr,
            ));
        }
        Some(SpecialProperty::Ignored | SpecialProperty::TypeAnnotation) => {
            return Ok(PropKind::Ignored);
        }
        _ => {}
    }

    if deserializer.flags.contains(SerdeStructFlags::OPEN_DATA)
        && deserializer
            .processor
            .profile
            .flags
            .contains(ProcessorProfileFlags::DESERIALIZE_OPEN_DATA)
    {
        Ok(PropKind::Open(v.into()))
    } else {
        // TODO: This error message could be improved to suggest valid fields.
        // see OneOf in serde (this is a private struct)
        Err(Error::custom(format!("unknown property `{v}`")))
    }
}
