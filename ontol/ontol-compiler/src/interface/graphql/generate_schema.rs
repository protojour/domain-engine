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
    ontology::Ontology,
    phf::PhfKey,
    resolve_path::ResolverGraph,
    DefId, DomainIndex, MapKey,
};
use tracing::{debug_span, trace};

use crate::{
    codegen::task::CodeCtx,
    interface::{graphql::schema_builder::QLevel, serde::serde_generator::SerdeGenerator},
    phf_build::build_phf_map,
    relation::UnionMemberCache,
};

use super::{
    graphql_namespace::{DomainDisambiguation, GraphqlNamespace},
    schema_builder::SchemaBuilder,
};

pub fn generate_graphql_schema<'c>(
    domain_index: DomainIndex,
    partial_ontology: &'c Ontology,
    map_namespace: Option<&'c IndexMap<&str, DefId>>,
    code_ctx: &'c CodeCtx,
    resolver_graph: &'c ResolverGraph,
    union_member_cache: &'c UnionMemberCache,
    serde_gen: &mut SerdeGenerator<'c, '_>,
) -> Option<GraphqlSchema> {
    let domain = partial_ontology.domain_by_index(domain_index).unwrap();

    let contains_entities = domain.defs().any(|def| def.entity().is_some());

    let mut named_maps: Vec<(&'c str, MapKey)> = vec![];

    if let Some(map_namespace) = map_namespace {
        // Register named maps in the user-specified order (using the IndexMap from the namespace)
        for name in map_namespace.keys() {
            let name_constant = serde_gen.str_ctx.intern_constant(name);

            if let Some(map_key) = code_ctx
                .result_named_downmaps
                .get(&(domain_index, name_constant))
            {
                named_maps.push((name, *map_key));
            }
        }
    }

    if !contains_entities && named_maps.is_empty() {
        // A domain with no queries or mutations doesn't get a GraphQL schema.
        return None;
    }

    let _entered = debug_span!("gql", pkg = ?domain_index.index()).entered();

    let mut schema = new_schema_with_capacity(domain_index, domain.type_count());
    let mut namespace = GraphqlNamespace::with_domain_disambiguation(DomainDisambiguation {
        root_domain: domain_index,
        ontology: partial_ontology,
    });

    let mut builder = {
        let rel_ctx = serde_gen.rel_ctx;
        let defs = serde_gen.defs;
        let prop_ctx = serde_gen.prop_ctx;
        let repr_ctx = serde_gen.repr_ctx;
        let misc_ctx = serde_gen.misc_ctx;
        SchemaBuilder {
            lazy_tasks: vec![],
            schema: &mut schema,
            type_namespace: &mut namespace,
            partial_ontology,
            serde_gen,
            rel_ctx,
            misc_ctx,
            defs,
            repr_ctx,
            prop_ctx,
            resolver_graph,
            union_member_cache,
            builtin_scalars: Default::default(),
        }
    };

    let mut query_fields: IndexMap<String, FieldData> = Default::default();
    let mut mutation_fields: IndexMap<String, FieldData> = Default::default();

    builder.register_fundamental_types(domain_index, partial_ontology);
    builder.register_standard_queries(&mut query_fields);

    for def in domain.defs() {
        if !def.public {
            continue;
        }

        if def.operator_addr.is_some() {
            trace!("adapt def `{ident:?}`", ident = def.ident());

            let type_ref = builder.gen_def_type_ref(def.id, QLevel::Node);

            if let Some(entity_data) = entity_check(builder.schema, type_ref) {
                builder.add_entity_queries_and_mutations(entity_data, &mut mutation_fields);
            }
        }
    }

    for (name, map_key) in named_maps {
        let prop_flow = code_ctx.result_propflow_table.get(&map_key).unwrap();

        builder.add_named_map_query(name, map_key, prop_flow, &mut query_fields);
    }

    builder.exec_lazy_tasks();

    builder.set_object_fields(builder.schema.query, query_fields);
    builder.set_object_fields(builder.schema.mutation, mutation_fields);

    schema.type_addr_by_typename =
        build_phf_map(schema.types.iter().enumerate().map(|(addr, data)| {
            let constant = data.typename;
            (
                PhfKey::new(constant, serde_gen.str_ctx[constant].into()),
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

fn new_schema_with_capacity(domain_index: DomainIndex, cap: usize) -> GraphqlSchema {
    GraphqlSchema {
        domain_index,
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
