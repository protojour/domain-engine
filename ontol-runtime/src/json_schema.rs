use std::collections::BTreeMap;

use fnv::FnvHashSet;
use serde::Serialize;
use serde::{ser::SerializeMap, ser::SerializeSeq, Serializer};
use smartstring::alias::String;

use crate::env::TypeInfo;
use crate::serde::SequenceRange;
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
) -> Option<StandaloneJsonSchema<'e>> {
    let mut graph_builder = SchemaGraphBuilder::default();

    graph_builder.visit(type_info.serde_operator_id?, env);

    let mut graph = graph_builder.graph;
    let operator_id = graph.remove(&type_info.def_id)?;

    Some(StandaloneJsonSchema {
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let package_id = self.package_id;
        let next_package_id = PackageId(self.package_id.0 + 1);
        let mut map = serializer.serialize_map(None)?;

        let ctx = SchemaCtx {
            env: &self.env,
            link_prefix: "#/components/schemas/",
        };

        // serialize schemas belonging to the domain package first
        for (def_id, operator_id) in self
            .schema_graph
            .range(DefId(package_id, 0)..DefId(next_package_id, 0))
        {
            map.serialize_entry(&ctx.format_key(*def_id), &ctx.schema(*operator_id, None))?;
        }

        for (def_id, operator_id) in &self.schema_graph {
            if def_id.0 != self.package_id {
                map.serialize_entry(&ctx.format_key(*def_id), &ctx.schema(*operator_id, None))?;
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ctx = SchemaCtx {
            env: &self.env,
            link_prefix: "#/$defs/",
        };

        let value_operator = &self.env.serde_operators[self.operator_id.0 as usize];

        JsonSchema {
            ctx,
            value_operator,
            rel_params_operator_id: None,
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
    env: &'e Env,
}

impl<'c, 'e> SchemaCtx<'c, 'e> {
    fn schema(
        self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> JsonSchema<'c, 'e> {
        let value_operator = &self.env.serde_operators[operator_id.0 as usize];

        JsonSchema {
            ctx: self,
            value_operator,
            rel_params_operator_id,
            defs: None,
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
    rel_params_operator_id: Option<SerdeOperatorId>,
    defs: Option<&'c BTreeMap<DefId, SerdeOperatorId>>,
}

impl<'c, 'e> JsonSchema<'c, 'e> {
    fn rel_link(&self, to: SerdeOperatorId) -> SchemaLink<'c, 'e> {
        SchemaLink {
            ctx: self.ctx,
            value_operator_id: to,
            rel_params_operator_id: self.rel_params_operator_id,
        }
    }

    fn items(&self, ranges: &'e [SequenceRange]) -> ArrayItems<'c, 'e> {
        ArrayItems {
            schema: *self,
            ranges,
        }
    }
}

impl<'c, 'e> Serialize for JsonSchema<'c, 'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        match self.value_operator {
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
            SerdeOperator::StringPattern(def_id)
            | SerdeOperator::CapturingStringPattern(def_id) => {
                let pattern = self.ctx.env.string_patterns.get(def_id).unwrap();
                map.serialize_entry("type", "string")?;
                map.serialize_entry("pattern", pattern.regex.as_str())?;
            }
            SerdeOperator::Sequence(ranges, _) => {
                map.serialize_entry("type", "array")?;
                match ranges.len() {
                    0 => {
                        map.serialize_entry::<str, [&'static str]>("items", &[])?;
                    }
                    1 => {
                        let range = ranges.iter().next().unwrap();
                        match &range.finite_repetition {
                            Some(_) => map.serialize_entry("items", &self.items(ranges))?,
                            None => {
                                map.serialize_entry("items", &self.rel_link(range.operator_id))?;
                            }
                        }
                    }
                    len => {
                        let last_range = ranges.last().unwrap();
                        if last_range.finite_repetition.is_some() {
                            map.serialize_entry("items", &self.items(ranges))?;
                        } else {
                            map.serialize_entry("items", &self.items(&ranges[..len - 1]))?;
                            map.serialize_entry(
                                "additionalItems",
                                &self.rel_link(last_range.operator_id),
                            )?;
                        }
                    }
                }
            }
            SerdeOperator::ValueType(_value_type) => todo!(),
            SerdeOperator::ValueUnionType(_value_union_type) => {
                todo!()
            }
            SerdeOperator::Id(_inner_operator_id) => {
                todo!()
            }
            SerdeOperator::MapType(_map_type) => {
                todo!()
            }
        };

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

#[derive(Clone, Copy)]
struct SchemaLink<'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    value_operator_id: SerdeOperatorId,
    rel_params_operator_id: Option<SerdeOperatorId>,
}

impl<'c, 'e> Serialize for SchemaLink<'c, 'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value_operator = &self.ctx.env.serde_operators[self.value_operator_id.0 as usize];

        let type_def_id = match value_operator {
            SerdeOperator::Sequence(_, type_def_id) => *type_def_id,
            SerdeOperator::ValueType(value_type) => value_type.type_def_id,
            SerdeOperator::ValueUnionType(value_union_type) => value_union_type.union_def_id,
            SerdeOperator::Id(_) => {
                todo!("id")
            }
            SerdeOperator::MapType(map_type) => map_type.type_def_id,
            SerdeOperator::Unit
            | SerdeOperator::Int(_)
            | SerdeOperator::Number(_)
            | SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::StringPattern(_)
            | SerdeOperator::CapturingStringPattern(_) => {
                panic!("Cannot link to this");
            }
        };

        let mut map = serializer.serialize_map(Some(1))?;
        if let Some(rel_params_operator_id) = self.rel_params_operator_id {
            map.serialize_entry(
                "allOf",
                &LinkUnion {
                    ctx: self.ctx,
                    to: &[self.value_operator_id, rel_params_operator_id],
                },
            )?;
        } else {
            map.serialize_entry("$ref", &self.ctx.format_link(type_def_id))?;
        }
        map.end()
    }
}

struct LinkUnion<'a, 'c, 'e> {
    ctx: SchemaCtx<'c, 'e>,
    to: &'a [SerdeOperatorId],
}

impl<'a, 'c, 'e> Serialize for LinkUnion<'a, 'c, 'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.to.len()))?;
        for link in self.to {
            seq.serialize_element(&SchemaLink {
                ctx: self.ctx,
                value_operator_id: *link,
                rel_params_operator_id: None,
            })?;
        }
        seq.end()
    }
}

struct ArrayItems<'c, 'e> {
    schema: JsonSchema<'c, 'e>,
    ranges: &'e [SequenceRange],
}

impl<'c, 'e> Serialize for ArrayItems<'c, 'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        for range in self.ranges {
            if let Some(repetition) = range.finite_repetition {
                for _ in 0..repetition {
                    seq.serialize_element(&self.schema.rel_link(range.operator_id))?;
                }
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        for (def_id, operator_id) in self.defs {
            map.serialize_entry(
                &self.ctx.format_key(*def_id),
                &self.ctx.schema(*operator_id, None),
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

        let operator = &env.serde_operators[operator_id.0 as usize];
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
            SerdeOperator::Id(operator_id) => {
                // id is not represented in the graph
                self.visit(*operator_id, env);
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
