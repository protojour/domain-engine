use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    config::data_store_backed_domains,
    interface::graphql::{
        data::{
            EntityData, NodeData, ObjectData, ObjectKind, TypeAddr, TypeData, TypeKind, UnitTypeRef,
        },
        schema::GraphqlSchema,
    },
    ontology::{MapLossiness, Ontology},
    resolve_path::ResolverGraph,
    DefId, PackageId,
};
use tracing::trace;

use crate::{
    codegen::task::CodegenTasks,
    interface::{graphql::builder::QLevel, serde::serde_generator::SerdeGenerator},
    primitive::Primitives,
};

use super::{
    builder::SchemaBuilder,
    graphql_namespace::{DomainDisambiguation, GraphqlNamespace},
};

pub fn generate_graphql_schema<'c>(
    package_id: PackageId,
    partial_ontology: &'c Ontology,
    primitives: &'c Primitives,
    map_namespace: Option<&'c IndexMap<&str, DefId>>,
    codegen_tasks: &'c CodegenTasks,
    serde_generator: &mut SerdeGenerator<'c, '_>,
) -> Option<GraphqlSchema> {
    let domain = partial_ontology.find_domain(package_id).unwrap();

    let data_store_domain = data_store_backed_domains(partial_ontology)
        .map(|(package_id, _)| package_id)
        .last();

    if !domain
        .type_names
        .iter()
        .any(|(_, def_id)| domain.type_info(*def_id).entity_info.is_some())
    {
        // A domain without entities doesn't get a GraphQL schema.
        return None;
    }

    let mut schema = new_schema_with_capacity(package_id, domain.type_names.len());
    let mut namespace = GraphqlNamespace::with_domain_disambiguation(DomainDisambiguation {
        root_domain: package_id,
        ontology: partial_ontology,
    });
    let mut builder = {
        let relations = serde_generator.relations;
        let defs = serde_generator.defs;
        let seal_ctx = serde_generator.seal_ctx;
        SchemaBuilder {
            lazy_tasks: vec![],
            schema: &mut schema,
            type_namespace: &mut namespace,
            partial_ontology,
            serde_generator,
            relations,
            defs,
            primitives,
            seal_ctx,
            resolver_graph: ResolverGraph::from_iter(
                codegen_tasks.result_map_proc_table.keys().map(|key| {
                    (
                        *key,
                        codegen_tasks
                            .result_metadata_table
                            .get(key)
                            .map(|meta| meta.lossiness)
                            .unwrap_or(MapLossiness::Lossy),
                    )
                }),
            ),
        }
    };

    builder.register_fundamental_types();

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);
        if !type_info.public {
            continue;
        }

        if type_info.operator_addr.is_some() {
            trace!("adapt type `{name:?}`", name = type_info.name);

            let type_ref = builder.get_def_type_ref(type_info.def_id, QLevel::Node);

            if let Some(entity_data) = entity_check(builder.schema, type_ref) {
                builder.add_entity_queries_and_mutations(entity_data, data_store_domain);
            }
        }
    }

    if let Some(map_namespace) = map_namespace {
        // Register named maps in the user-specified order (using the IndexMap from the namespace)
        for name in map_namespace.keys() {
            if let Some(map_key) = codegen_tasks
                .result_named_forward_maps
                .get(&(package_id, (*name).into()))
            {
                let prop_flow = codegen_tasks.result_propflow_table.get(map_key).unwrap();

                builder.add_named_map_query(name, map_key, prop_flow);
            }
        }
    }

    builder.exec_lazy_tasks();

    Some(schema)
}

fn entity_check(schema: &GraphqlSchema, type_ref: UnitTypeRef) -> Option<EntityData> {
    if let UnitTypeRef::Addr(type_addr) = type_ref {
        let type_data = &schema.type_data(type_addr);

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
                type_addr,
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

fn new_schema_with_capacity(package_id: PackageId, cap: usize) -> GraphqlSchema {
    GraphqlSchema {
        package_id,
        query: TypeAddr(0),
        mutation: TypeAddr(0),
        page_info: TypeAddr(0),
        i64_custom_scalar: None,
        json_scalar: TypeAddr(0),
        types: Vec::with_capacity(cap),
        type_addr_by_def: FnvHashMap::with_capacity_and_hasher(cap, Default::default()),
    }
}
