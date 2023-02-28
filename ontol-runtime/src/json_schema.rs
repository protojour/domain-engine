use std::collections::{BTreeMap, HashMap};

use fnv::FnvHashSet;
use serde::Serialize;
use serde::{ser::SerializeMap, ser::SerializeSeq, Serializer};
use smartstring::alias::String;

use crate::env::TypeInfo;
use crate::serde::{MapType, SequenceRange, ValueUnionType};
use crate::smart_format;
use crate::{
    env::{Domain, Env},
    serde::{SerdeOperator, SerdeOperatorId},
    DefId, PackageId,
};

pub fn build_openapi_schemas<'e>(
    env: &'e Env,
    package_id: PackageId,
    domain: &'e Domain,
) -> OpenApiSchemas<'e> {
    let mut graph_builder = SchemaGraphBuilder::default();

    for (_, type_info) in &domain.types {
        if let Some(operator_id) = &type_info.serde_operator_id {
            graph_builder.visit(*operator_id, env);
        }
    }

    OpenApiSchemas {
        schema_graph: graph_builder.graph,
        package_id,
        env,
    }
}

pub fn build_standalone_schema<'e>(
    env: &'e Env,
    type_info: &TypeInfo,
) -> Result<StandaloneJsonSchema<'e>, &'static str> {
    let mut graph_builder = SchemaGraphBuilder::default();

    let operator_id = type_info
        .serde_operator_id
        .ok_or("no serde operator id available")?;
    graph_builder.visit(operator_id, env);

    let mut graph = graph_builder.graph;
    let operator_id = graph.remove(&type_info.def_id).unwrap_or(operator_id);

    Ok(StandaloneJsonSchema {
        operator_id,
        defs: graph,
        env,
    })
}

pub struct OpenApiSchemas<'e> {
    schema_graph: BTreeMap<DefId, SerdeOperatorId>,
    package_id: PackageId,
    env: &'e Env,
}

impl<'e> Serialize for OpenApiSchemas<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let package_id = self.package_id;
        let next_package_id = PackageId(self.package_id.0 + 1);
        let mut map = serializer.serialize_map(None)?;

        let ctx = SchemaCtx {
            env: &self.env,
            rel_params_operator_id: None,
            link_prefix: "#/components/schemas/",
        };

        // serialize schemas belonging to the domain package first
        for (def_id, operator_id) in self
            .schema_graph
            .range(DefId(package_id, 0)..DefId(next_package_id, 0))
        {
            map.serialize_entry(&ctx.format_key(*def_id), &ctx.schema(*operator_id))?;
        }

        for (def_id, operator_id) in &self.schema_graph {
            if def_id.0 != self.package_id {
                map.serialize_entry(&ctx.format_key(*def_id), &ctx.schema(*operator_id))?;
            }
        }

        map.end()
    }
}

pub struct StandaloneJsonSchema<'e> {
    operator_id: SerdeOperatorId,
    defs: BTreeMap<DefId, SerdeOperatorId>,
    env: &'e Env,
}

impl<'e> Serialize for StandaloneJsonSchema<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let ctx = SchemaCtx {
            env: &self.env,
            rel_params_operator_id: None,
            link_prefix: "#/$defs/",
        };

        JsonSchema {
            ctx,
            value_operator: self.env.get_serde_operator(self.operator_id),
            defs: if self.defs.is_empty() {
                None
            } else {
                Some(&self.defs)
            },
        }
        .serialize(serializer)
    }
}

#[derive(Clone, Copy)]
struct SchemaCtx<'c, 'e> {
    link_prefix: &'c str,
    rel_params_operator_id: Option<SerdeOperatorId>,
    env: &'e Env,
}

impl<'c, 'e> SchemaCtx<'c, 'e> {
    fn schema(self, operator_id: SerdeOperatorId) -> JsonSchema<'c, 'e> {
        JsonSchema {
            ctx: self,
            value_operator: self.env.get_serde_operator(operator_id),
            defs: None,
        }
    }

    fn reference(&self, link: SerdeOperatorId) -> SchemaReference<'c, 'e> {
        SchemaReference { ctx: *self, link }
    }

    fn items_ref_links(&self, ranges: &'e [SequenceRange]) -> ArrayItemsRefLinks<'c, 'e> {
        ArrayItemsRefLinks { ctx: *self, ranges }
    }

    /// Derive a new context with rel_params_operator_id removed.
    /// This is important when serializing properties
    fn reset(&self) -> Self {
        SchemaCtx {
            link_prefix: self.link_prefix,
            rel_params_operator_id: None,
            env: self.env,
        }
    }

    fn format_key(&self, def_id: DefId) -> String {
        smart_format!("{}_{}", def_id.0 .0, def_id.1)
    }

    fn format_link(&self, def_id: DefId) -> String {
        smart_format!("{}{}_{}", self.link_prefix, def_id.0 .0, def_id.1)
    }
}

#[derive(Clone, Copy)]
struct JsonSchema<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    value_operator: &'e SerdeOperator,
    defs: Option<&'c BTreeMap<DefId, SerdeOperatorId>>,
}

impl<'c, 'e> JsonSchema<'c, 'e> {}

impl<'c, 'e> Serialize for JsonSchema<'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        serialize_schema::<S>(&self.ctx, self.value_operator, None, &mut map)?;

        if let Some(defs) = self.defs {
            map.serialize_entry(
                "$defs",
                &Defs {
                    ctx: self.ctx,
                    defs,
                },
            )?;
        }

        map.end()
    }
}

/// A link to another schema
#[derive(Clone, Copy)]
struct SchemaReference<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    link: SerdeOperatorId,
}

impl<'c, 'e> Serialize for SchemaReference<'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value_operator = &self.ctx.env.get_serde_operator(self.link);

        match value_operator {
            SerdeOperator::Unit
            | SerdeOperator::Int(_)
            | SerdeOperator::Number(_)
            | SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::StringPattern(_)
            | SerdeOperator::CapturingStringPattern(_) => {
                // These are inline schemas
                let mut map = serializer.serialize_map(None)?;
                serialize_schema::<S>(&self.ctx, value_operator, None, &mut map)?;
                map.end()
            }
            SerdeOperator::Sequence(_, type_def_id) => {
                serialize_link(&self.ctx, self.link, *type_def_id, serializer)
            }
            SerdeOperator::ValueType(value_type) => {
                serialize_link(&self.ctx, self.link, value_type.type_def_id, serializer)
            }
            SerdeOperator::ValueUnionType(value_union_type) => serialize_link(
                &self.ctx,
                self.link,
                value_union_type.union_def_id,
                serializer,
            ),
            SerdeOperator::Id(id_operator_id) => {
                // This is a special inline schema
                // BUG: Need allOf in case of rel_params
                let mut map = serializer.serialize_map(None)?;

                let properties: HashMap<_, _> =
                    [("_id", self.ctx.reference(*id_operator_id))].into();

                map.serialize_entry("type", "object")?;
                map.serialize_entry("properties", &properties)?;
                map.serialize_entry("required", &["_id"])?;

                map.end()
            }
            SerdeOperator::MapType(map_type) => {
                serialize_link(&self.ctx, self.link, map_type.type_def_id, serializer)
            }
        }
    }
}

fn serialize_link<'c, 'e, S: Serializer>(
    ctx: &SchemaCtx<'c, 'e>,
    link: SerdeOperatorId,
    type_def_id: DefId,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(1))?;
    if let Some(rel_params_operator_id) = ctx.rel_params_operator_id {
        map.serialize_entry(
            "allOf",
            &RefLinkSlice {
                // Remove the rel params to avoid infinite loop!
                ctx: ctx.reset(),
                links: &[link, rel_params_operator_id],
            },
        )?;
    } else {
        map.serialize_entry("$ref", &ctx.format_link(type_def_id))?;
    }
    map.end()
}

fn serialize_schema<'c, 'e, S: Serializer>(
    ctx: &SchemaCtx<'c, 'e>,
    value_operator: &'e SerdeOperator,
    _description: Option<&str>,
    map: &mut <S as Serializer>::SerializeMap,
) -> Result<(), S::Error> {
    match value_operator {
        SerdeOperator::Unit => {
            map.serialize_entry("type", "object")?;
        }
        // FIXME: Distinguish different number types
        SerdeOperator::Int(_) | SerdeOperator::Number(_) => {
            map.serialize_entry("type", "integer")?;
            map.serialize_entry("format", "int64")?;
        }
        SerdeOperator::String(_) => {
            map.serialize_entry("type", "string")?;
        }
        SerdeOperator::StringConstant(literal, _) => {
            map.serialize_entry("type", "string")?;
            map.serialize_entry("enum", &[literal])?;
        }
        SerdeOperator::StringPattern(def_id) | SerdeOperator::CapturingStringPattern(def_id) => {
            let pattern = ctx.env.string_patterns.get(def_id).unwrap();
            map.serialize_entry("type", "string")?;
            map.serialize_entry("pattern", pattern.regex.as_str())?;
        }
        SerdeOperator::Sequence(ranges, _) => {
            map.serialize_entry("type", "array")?;

            if ranges.is_empty() {
                map.serialize_entry::<str, [&'static str]>("items", &[])?;
            } else {
                let len = ranges.len();
                let last_range = ranges.last().unwrap();

                if last_range.finite_repetition.is_some() {
                    // a finite sequence/tuple
                    map.serialize_entry("items", &ctx.items_ref_links(ranges))?;
                } else if len == 1 {
                    // normal array: all items are uniform
                    map.serialize_entry("items", &ctx.reference(last_range.operator_id))?;
                } else {
                    // tuple followed by array
                    map.serialize_entry("items", &ctx.items_ref_links(&ranges[..len - 1]))?;
                    map.serialize_entry("additionalItems", &ctx.reference(last_range.operator_id))?;
                }
            }
        }
        SerdeOperator::ValueType(value_type) => {
            serialize_schema::<S>(
                ctx,
                &ctx.env.get_serde_operator(value_type.inner_operator_id),
                Some("TODO: overridden description"),
                map,
            )?;
        }
        SerdeOperator::ValueUnionType(value_union_type) => {
            map.serialize_entry(
                "oneOf",
                &UnionRefLinks {
                    ctx: *ctx,
                    value_union_type,
                },
            )?;
        }
        SerdeOperator::Id(_inner_operator_id) => {
            panic!("BUG: Id not handled here")
        }
        SerdeOperator::MapType(map_type) => {
            map.serialize_entry("type", "object")?;
            map.serialize_entry(
                "properties",
                &MapProperties {
                    ctx: ctx.reset(),
                    map_type,
                },
            )?;
            if map_type.n_mandatory_properties > 0 {
                map.serialize_entry("required", &RequiredMapProperties { map_type })?;
            }
        }
    };

    Ok(())
}

struct UnionRefLinks<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    value_union_type: &'e ValueUnionType,
}

impl<'c, 'e> Serialize for UnionRefLinks<'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;

        for discriminator in &self.value_union_type.discriminators {
            seq.serialize_element(&self.ctx.reference(discriminator.operator_id))?;
        }

        seq.end()
    }
}

struct RefLinkSlice<'a, 'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    links: &'a [SerdeOperatorId],
}

impl<'a, 'c, 'e> Serialize for RefLinkSlice<'a, 'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.links.len()))?;
        for link in self.links {
            seq.serialize_element(&self.ctx.reference(*link))?;
        }
        seq.end()
    }
}

struct ArrayItemsRefLinks<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    ranges: &'e [SequenceRange],
}

impl<'c, 'e> Serialize for ArrayItemsRefLinks<'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for range in self.ranges {
            if let Some(repetition) = range.finite_repetition {
                for _ in 0..repetition {
                    seq.serialize_element(&self.ctx.reference(range.operator_id))?;
                }
            }
        }
        seq.end()
    }
}

struct MapProperties<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    map_type: &'e MapType,
}

impl<'c, 'e> Serialize for MapProperties<'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (key, property) in &self.map_type.properties {
            // FIXME: Need to merge the documentation of the _property_
            // and the documentation of the type
            map.serialize_entry(key, &self.ctx.reference(property.value_operator_id))?;
        }
        map.end()
    }
}

struct RequiredMapProperties<'e> {
    map_type: &'e MapType,
}

impl<'e> Serialize for RequiredMapProperties<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for (key, property) in &self.map_type.properties {
            if !property.optional {
                seq.serialize_element(key)?;
            }
        }
        seq.end()
    }
}

struct Defs<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    defs: &'c BTreeMap<DefId, SerdeOperatorId>,
}

impl<'c, 'e> Serialize for Defs<'c, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        for (def_id, operator_id) in self.defs {
            map.serialize_entry(
                &self.ctx.format_key(*def_id),
                &self.ctx.schema(*operator_id),
            )?;
        }

        map.end()
    }
}

#[derive(Default)]
struct SchemaGraphBuilder {
    graph: BTreeMap<DefId, SerdeOperatorId>,
    visited: FnvHashSet<SerdeOperatorId>,
}

impl SchemaGraphBuilder {
    fn visit(&mut self, operator_id: SerdeOperatorId, env: &Env) {
        if !self.mark_visited(operator_id) {
            return;
        }

        let operator = &env.get_serde_operator(operator_id);
        match operator {
            SerdeOperator::Unit
            | SerdeOperator::Int(_)
            | SerdeOperator::Number(_)
            | SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::StringPattern(_)
            | SerdeOperator::CapturingStringPattern(_) => {}
            SerdeOperator::Sequence(ranges, def_id) => {
                self.graph.insert(*def_id, operator_id);
                for range in ranges {
                    self.visit(range.operator_id, env);
                }
            }
            SerdeOperator::ValueType(value_type) => {
                self.graph.insert(value_type.type_def_id, operator_id);
                self.visit(value_type.inner_operator_id, env);
            }
            SerdeOperator::ValueUnionType(value_union_type) => {
                self.graph
                    .insert(value_union_type.union_def_id, operator_id);

                for discriminator in &value_union_type.discriminators {
                    self.visit(discriminator.operator_id, env);
                }
            }
            SerdeOperator::Id(id_operator_id) => {
                // id is not represented in the graph
                self.visit(*id_operator_id, env);
            }
            SerdeOperator::MapType(map_type) => {
                self.graph.insert(map_type.type_def_id, operator_id);

                for (_, property) in &map_type.properties {
                    self.visit(property.value_operator_id, env);
                    if let Some(operator_id) = &property.rel_params_operator_id {
                        self.visit(*operator_id, env);
                    }
                }
            }
        }
    }

    fn mark_visited(&mut self, operator_id: SerdeOperatorId) -> bool {
        if self.visited.contains(&operator_id) {
            false
        } else {
            self.visited.insert(operator_id);
            true
        }
    }
}
