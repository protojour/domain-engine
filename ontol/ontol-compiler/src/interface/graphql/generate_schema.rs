use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    interface::graphql::{
        data::{
            EntityData, FieldData, NodeData, ObjectData, ObjectKind, TypeAddr, TypeData, TypeKind,
            UnitTypeRef,
        },
        schema::GraphqlSchema,
    },
    ontology::{config::data_store_backed_domains, map::MapLossiness, Ontology},
    phf::PhfKey,
    resolve_path::ResolverGraph,
    DefId, MapDirection, MapKey, PackageId,
};
use tracing::{debug_span, trace};

use crate::{
    codegen::task::CodegenTasks,
    interface::{graphql::schema_builder::QLevel, serde::serde_generator::SerdeGenerator},
    phf_build::build_phf_map,
    primitive::Primitives,
    relation::UnionMemberCache,
};

use super::{
    graphql_namespace::{DomainDisambiguation, GraphqlNamespace},
    schema_builder::SchemaBuilder,
};

pub fn generate_graphql_schema<'c>(
    package_id: PackageId,
    partial_ontology: &'c Ontology,
    primitives: &'c Primitives,
    map_namespace: Option<&'c IndexMap<&str, DefId>>,
    codegen_tasks: &'c CodegenTasks,
    union_member_cache: &'c UnionMemberCache,
    serde_gen: &mut SerdeGenerator<'c, '_>,
) -> Option<GraphqlSchema> {
    let domain = partial_ontology.find_domain(package_id).unwrap();

    let data_store_domain = data_store_backed_domains(partial_ontology)
        .map(|(package_id, _)| package_id)
        .last();

    let contains_entities = domain
        .type_infos()
        .any(|type_info| type_info.entity_info().is_some());

    let mut named_maps: Vec<(&'c str, MapKey)> = vec![];

    if let Some(map_namespace) = map_namespace {
        // Register named maps in the user-specified order (using the IndexMap from the namespace)
        for name in map_namespace.keys() {
            let name_constant = serde_gen.strings.intern_constant(name);

            if let Some(map_key) = codegen_tasks
                .result_named_downmaps
                .get(&(package_id, name_constant))
            {
                named_maps.push((name, *map_key));
            }
        }
    }

    if !contains_entities && named_maps.is_empty() {
        // A domain with no queries or mutations doesn't get a GraphQL schema.
        return None;
    }

    let _entered = debug_span!("gql", pkg = ?package_id.0).entered();

    let mut schema = new_schema_with_capacity(package_id, domain.type_count());
    let mut namespace = GraphqlNamespace::with_domain_disambiguation(DomainDisambiguation {
        root_domain: package_id,
        ontology: partial_ontology,
    });

    let mut builder = {
        let relations = serde_gen.relations;
        let defs = serde_gen.defs;
        let repr_ctx = serde_gen.repr_ctx;
        SchemaBuilder {
            lazy_tasks: vec![],
            schema: &mut schema,
            type_namespace: &mut namespace,
            partial_ontology,
            serde_gen,
            relations,
            defs,
            primitives,
            repr_ctx,
            resolver_graph: ResolverGraph::new(codegen_tasks.result_map_proc_table.keys().map(
                |key| {
                    codegen_tasks
                        .result_metadata_table
                        .get(key)
                        .map(|meta| (*key, meta.direction, meta.lossiness))
                        .unwrap_or((*key, MapDirection::Down, MapLossiness::Lossy))
                },
            )),
            union_member_cache,
            builtin_scalars: Default::default(),
        }
    };

    let mut query_fields: IndexMap<String, FieldData> = Default::default();
    let mut mutation_fields: IndexMap<String, FieldData> = Default::default();

    builder.register_fundamental_types();
    builder.register_standard_queries(&mut query_fields);

    for type_info in domain.type_infos() {
        if !type_info.public {
            continue;
        }

        if type_info.operator_addr.is_some() {
            trace!("adapt type `{name:?}`", name = type_info.name());

            let type_ref = builder.get_def_type_ref(type_info.def_id, QLevel::Node);

            if let Some(entity_data) = entity_check(builder.schema, type_ref) {
                builder.add_entity_queries_and_mutations(
                    entity_data,
                    data_store_domain,
                    &mut mutation_fields,
                );
            }
        }
    }

    for (name, map_key) in named_maps {
        let prop_flow = codegen_tasks.result_propflow_table.get(&map_key).unwrap();

        builder.add_named_map_query(name, map_key, prop_flow, &mut query_fields);
    }

    builder.exec_lazy_tasks();

    builder.set_object_fields(builder.schema.query, query_fields);
    builder.set_object_fields(builder.schema.mutation, mutation_fields);

    schema.type_addr_by_typename =
        build_phf_map(schema.types.iter().enumerate().map(|(addr, data)| {
            let constant = data.typename;
            (
                PhfKey::new(constant, serde_gen.strings[constant].into()),
                TypeAddr(addr as u32),
            )
        }));

    Some(schema)
}

fn entity_check(schema: &GraphqlSchema, type_ref: UnitTypeRef) -> Option<EntityData> {
    if let UnitTypeRef::Addr(type_addr) = type_ref {
        let type_data = schema.type_data(type_addr);

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
        type_addr_by_typename: Default::default(),
        interface_implementors: Default::default(),
    }
}
