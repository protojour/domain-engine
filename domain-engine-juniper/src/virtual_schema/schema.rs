use std::sync::Arc;

use fnv::FnvHashMap;
use ontol_runtime::{
    discriminator::Discriminant,
    ontology::Ontology,
    serde::{
        operator::{FilteredVariants, SerdeOperator, SerdeOperatorId},
        processor::{ProcessorLevel, ProcessorMode},
    },
    DefId, PackageId,
};
use tracing::debug;

use crate::SchemaBuildError;

use super::{
    builder::VirtualSchemaBuilder,
    data::{
        EntityData, NativeScalarRef, NodeData, ObjectData, ObjectKind, TypeData, TypeIndex,
        TypeKind, UnitTypeRef,
    },
    namespace::{DomainDisambiguation, Namespace},
    QueryLevel, TypingPurpose, VirtualIndexedTypeInfo,
};

/// The virtual schema is a schema representation
/// held as a "shadow" schema behind juniper's user-facing schema.
///
/// This comes in handy when performing lookahead on selection sets,
/// parsing field arguments, etc.
pub struct VirtualSchema {
    pub(super) ontology: Arc<Ontology>,
    pub(super) package_id: PackageId,
    pub(super) query: TypeIndex,
    pub(super) mutation: TypeIndex,
    pub(super) types: Vec<TypeData>,
    pub(super) type_index_by_def: FnvHashMap<(DefId, QueryLevel), TypeIndex>,
}

impl VirtualSchema {
    /// Builds a "shadow schema" before handing that over to juniper
    pub fn build(package_id: PackageId, ontology: Arc<Ontology>) -> Result<Self, SchemaBuildError> {
        let domain = ontology
            .find_domain(package_id)
            .ok_or(SchemaBuildError::UnknownPackage)?;

        let mut schema = Self {
            ontology: ontology.clone(),
            package_id,
            query: TypeIndex(0),
            mutation: TypeIndex(0),
            types: Vec::with_capacity(domain.type_names.len()),
            type_index_by_def: FnvHashMap::with_capacity_and_hasher(
                domain.type_names.len(),
                Default::default(),
            ),
        };
        let mut namespace = Namespace::with_domain_disambiguation(DomainDisambiguation {
            root_domain: package_id,
            ontology: ontology.clone(),
        });
        let mut builder = VirtualSchemaBuilder {
            ontology: ontology.as_ref(),
            schema: &mut schema,
            namespace: &mut namespace,
        };

        builder.register_query();
        builder.register_mutation();

        for (_, def_id) in &domain.type_names {
            let type_info = domain.type_info(*def_id);
            if !type_info.public {
                continue;
            }

            if let Some(operator_id) = type_info.operator_id {
                debug!(
                    "adapt type `{name:?}` {operator_id:?}",
                    name = type_info.name
                );

                let type_ref = builder.get_def_type_ref(type_info.def_id, QueryLevel::Node);

                if let Some(entity_info) = builder.schema.entity_check(type_ref) {
                    builder.add_entity_queries_and_mutations(entity_info);
                }
            }
        }

        Ok(schema)
    }

    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn package_id(&self) -> PackageId {
        self.package_id
    }

    pub fn indexed_type_info(
        self: &Arc<Self>,
        type_index: TypeIndex,
        typing_purpose: TypingPurpose,
    ) -> VirtualIndexedTypeInfo {
        VirtualIndexedTypeInfo {
            virtual_schema: self.clone(),
            type_index,
            typing_purpose,
        }
    }

    pub fn indexed_type_info_by_unit(
        self: &Arc<Self>,
        type_ref: UnitTypeRef,
        typing_purpose: TypingPurpose,
    ) -> Result<VirtualIndexedTypeInfo, NativeScalarRef> {
        self.lookup_type_index(type_ref)
            .map(|type_index| VirtualIndexedTypeInfo {
                virtual_schema: self.clone(),
                type_index,
                typing_purpose,
            })
    }

    pub fn query_type_info(self: &Arc<Self>) -> VirtualIndexedTypeInfo {
        self.indexed_type_info(self.query, TypingPurpose::Selection)
    }

    pub fn mutation_type_info(self: &Arc<Self>) -> VirtualIndexedTypeInfo {
        self.indexed_type_info(self.mutation, TypingPurpose::Selection)
    }

    pub fn type_data(&self, type_index: TypeIndex) -> &TypeData {
        &self.types[type_index.0 as usize]
    }

    pub fn lookup_type_index(&self, type_ref: UnitTypeRef) -> Result<TypeIndex, NativeScalarRef> {
        match type_ref {
            UnitTypeRef::Indexed(type_index) => Ok(type_index),
            UnitTypeRef::NativeScalar(scalar_ref) => Err(scalar_ref),
        }
    }

    pub fn lookup_type_data(&self, type_ref: UnitTypeRef) -> Result<&TypeData, NativeScalarRef> {
        self.lookup_type_index(type_ref)
            .map(|type_index| self.type_data(type_index))
    }

    pub fn type_index_by_def(&self, def_id: DefId, query_level: QueryLevel) -> Option<TypeIndex> {
        self.type_index_by_def.get(&(def_id, query_level)).cloned()
    }

    pub(super) fn object_data_mut(&mut self, index: TypeIndex) -> &mut ObjectData {
        let type_data = self.types.get_mut(index.0 as usize).unwrap();
        match &mut type_data.kind {
            TypeKind::Object(object_data) => object_data,
            _ => panic!("{index:?} is not an object"),
        }
    }

    fn entity_check(&self, type_ref: UnitTypeRef) -> Option<EntityData> {
        if let UnitTypeRef::Indexed(type_index) = type_ref {
            let type_data = &self.type_data(type_index);

            if let TypeData {
                kind:
                    TypeKind::Object(ObjectData {
                        kind:
                            ObjectKind::Node(NodeData {
                                def_id: node_def_id,
                                entity_id: Some(id_def_id),
                                ..
                            }),
                        ..
                    }),
                ..
            } = type_data
            {
                Some(EntityData {
                    type_index,
                    node_def_id: *node_def_id,
                    id_def_id: *id_def_id,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub enum TypeClassification {
    Type(NodeClassification, DefId, SerdeOperatorId),
    Id,
    NativeScalar,
}

pub enum NodeClassification {
    Node,
    Entity,
}

pub fn classify_type(ontology: &Ontology, operator_id: SerdeOperatorId) -> TypeClassification {
    let operator = ontology.get_serde_operator(operator_id);
    // debug!("    classify operator: {operator:?}");

    match operator {
        SerdeOperator::Struct(struct_op) => {
            let type_info = ontology.get_type_info(struct_op.def_variant.def_id);
            let node_classification = if type_info.entity_info.is_some() {
                NodeClassification::Entity
            } else {
                NodeClassification::Node
            };
            TypeClassification::Type(
                node_classification,
                struct_op.def_variant.def_id,
                operator_id,
            )
        }
        SerdeOperator::Union(union_op) => {
            match union_op.variants(ProcessorMode::Inspect, ProcessorLevel::new_child()) {
                FilteredVariants::Single(operator_id) => classify_type(ontology, operator_id),
                FilteredVariants::Union(variants) => {
                    // start with the "highest" classification and downgrade as "lower" variants are found.
                    let mut classification = TypeClassification::Type(
                        NodeClassification::Entity,
                        union_op.union_def_variant().def_id,
                        operator_id,
                    );

                    for variant in variants {
                        if variant.discriminator.discriminant == Discriminant::MapFallback {
                            panic!("BUG: Don't want to see this variant in ProcessorMode::Inspect");
                        }

                        let variant_classification = classify_type(ontology, variant.operator_id);
                        match (&classification, variant_classification) {
                            (
                                TypeClassification::Type(..),
                                TypeClassification::Type(NodeClassification::Node, ..),
                            ) => {
                                // downgrade
                                debug!("    Downgrade to Node");
                                classification = TypeClassification::Type(
                                    NodeClassification::Node,
                                    union_op.union_def_variant().def_id,
                                    operator_id,
                                );
                            }
                            (_, TypeClassification::NativeScalar) => {
                                // downgrade
                                debug!("    Downgrade to Scalar");
                                classification = TypeClassification::NativeScalar;
                            }
                            _ => {}
                        }
                    }

                    classification
                }
            }
        }
        SerdeOperator::PrimaryId(..) => TypeClassification::Id,
        SerdeOperator::ConstructorSequence(seq_op) => TypeClassification::Type(
            NodeClassification::Node,
            seq_op.def_variant.def_id,
            operator_id,
        ),
        operator => {
            debug!("    operator interpreted as Scalar: {operator:?}");
            TypeClassification::NativeScalar
        }
    }
}
