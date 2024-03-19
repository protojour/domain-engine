//! Domain description model, part of the Ontology

use fnv::FnvHashMap;
use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};

use crate::{
    interface::serde::operator::SerdeOperatorAddr,
    property::{Cardinality, PropertyId, Role},
    query::order::Direction,
    DefId, RelationshipId,
};

use super::ontol::{TextConstant, ValueGenerator};

/// A domain in the ONTOL ontology.
#[derive(Serialize, Deserialize)]
pub struct Domain {
    unique_name: TextConstant,

    /// Types by DefId.1 (the type's index within the domain)
    info: Vec<TypeInfo>,
}

impl Domain {
    pub fn new(unique_name: TextConstant) -> Self {
        Self {
            unique_name,
            info: Default::default(),
        }
    }

    pub fn unique_name(&self) -> TextConstant {
        self.unique_name
    }

    pub fn type_count(&self) -> usize {
        self.info.len()
    }

    pub fn type_info(&self, def_id: DefId) -> &TypeInfo {
        &self.info[def_id.1 as usize]
    }

    pub fn type_infos(&self) -> impl Iterator<Item = &TypeInfo> {
        self.info.iter()
    }

    pub fn find_type_by_name(&self, name: TextConstant) -> Option<&TypeInfo> {
        self.info
            .iter()
            .find(|type_info| type_info.name() == Some(name))
    }

    pub fn add_type(&mut self, type_info: TypeInfo) {
        self.register_type_info(type_info);
    }

    fn register_type_info(&mut self, type_info: TypeInfo) {
        let index = type_info.def_id.1 as usize;

        // pad the vector
        let new_size = std::cmp::max(self.info.len(), index + 1);
        self.info.resize_with(new_size, || TypeInfo {
            def_id: DefId(type_info.def_id.0, 0),
            public: false,
            kind: TypeKind::Data(BasicTypeInfo { name: None }),
            operator_addr: None,
            data_relationships: Default::default(),
        });

        self.info[index] = type_info;
    }

    pub fn find_type_info_by_name(&self, name: TextConstant) -> Option<&TypeInfo> {
        self.info
            .iter()
            .find(|type_info| type_info.name() == Some(name))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    pub def_id: DefId,
    pub kind: TypeKind,
    pub public: bool,
    /// The SerdeOperatorAddr used for JSON.
    /// FIXME: This should really be connected to a DomainInterface.
    pub operator_addr: Option<SerdeOperatorAddr>,

    pub data_relationships: FnvHashMap<PropertyId, DataRelationshipInfo>,
}

impl TypeInfo {
    pub fn name(&self) -> Option<TextConstant> {
        match &self.kind {
            TypeKind::Entity(info) => Some(info.name),
            TypeKind::Data(info)
            | TypeKind::Relationship(info)
            | TypeKind::Function(info)
            | TypeKind::Domain(info)
            | TypeKind::Generator(info) => info.name,
        }
    }

    pub fn entity_info(&self) -> Option<&EntityInfo> {
        match &self.kind {
            TypeKind::Entity(entity_info) => Some(entity_info),
            _ => None,
        }
    }

    pub fn entity_relationships(
        &self,
    ) -> impl Iterator<Item = (&PropertyId, &DataRelationshipInfo)> {
        self.data_relationships
            .iter()
            .filter(|(_, info)| matches!(info.kind, DataRelationshipKind::EntityGraph { .. }))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TypeKind {
    Entity(EntityInfo),
    Data(BasicTypeInfo),
    Relationship(BasicTypeInfo),
    Function(BasicTypeInfo),
    Domain(BasicTypeInfo),
    Generator(BasicTypeInfo),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct BasicTypeInfo {
    pub name: Option<TextConstant>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EntityInfo {
    pub name: TextConstant,
    pub id_relationship_id: RelationshipId,
    pub id_value_def_id: DefId,
    pub id_operator_addr: SerdeOperatorAddr,
    /// Whether all inherent fields are part of the primary id of this entity.
    /// In other words: The entity has only one field, its ID.
    pub is_self_identifying: bool,
    pub id_value_generator: Option<ValueGenerator>,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct DataRelationshipInfo {
    pub kind: DataRelationshipKind,
    pub subject_cardinality: Cardinality,
    pub object_cardinality: Cardinality,
    pub subject_name: TextConstant,
    pub object_name: Option<TextConstant>,
    pub source: DataRelationshipSource,
    pub target: DataRelationshipTarget,
}

impl DataRelationshipInfo {
    pub fn cardinality_by_role(&self, role: Role) -> Cardinality {
        match role {
            Role::Subject => self.subject_cardinality,
            Role::Object => self.object_cardinality,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipKind {
    /// The relationship is between an entity and its identifier
    Id,
    /// A Tree data relationship that can never be circular.
    /// It expresses a simple composition of a composite (the parent) and the component (the child).
    Tree,
    /// Graph data relationships can be circular and involves entities.
    /// The Graph relationship kind must go from one entity to another entity.
    EntityGraph {
        /// EntityGraph data relationships are allowed to be parametric.
        /// i.e. the relation itself has parameters.
        rel_params: Option<DefId>,
    },
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipSource {
    Inherent,
    ByUnionProxy,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipTarget {
    Unambiguous(DefId),
    /// The target is a union of types, only known during runtime.
    /// The union's variants are accessed using [Ontology::union_variants].
    Union(DefId),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ExtendedEntityInfo {
    pub order_union: Option<DefId>,

    /// The `order` definitions on the entity.
    pub order_table: FnvHashMap<DefId, EntityOrder>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EntityOrder {
    pub tuple: Box<[FieldPath]>,
    pub direction: Direction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldPath(pub Box<[PropertyId]>);
