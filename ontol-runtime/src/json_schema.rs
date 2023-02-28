use std::collections::BTreeMap;

use fnv::FnvHashSet;
use serde::Serialize;
use serde::{ser::SerializeMap, ser::SerializeSeq, Serializer};

use crate::serde::SequenceRange;
use crate::{
    env::{Domain, Env},
    serde::{SerdeOperator, SerdeOperatorId},
    DefId, PackageId,
};

pub struct DomainJsonSchemas<'e> {
    schema_graph: BTreeMap<DefId, SerdeOperatorId>,
    package_id: PackageId,
    domain: &'e Domain,
    env: &'e Env,
}

impl<'e> DomainJsonSchemas<'e> {
    pub fn build(env: &'e Env, package_id: PackageId, domain: &'e Domain) -> Self {
        let mut graph_builder = SchemaGraphBuilder {
            graph: Default::default(),
            visited: Default::default(),
        };

        for (_, type_info) in &domain.types {
            if let Some(operator_id) = &type_info.serde_operator_id {
                graph_builder.visit(*operator_id, env);
            }
        }

        Self {
            schema_graph: graph_builder.graph,
            package_id,
            env,
            domain,
        }
    }
}

impl<'e> Serialize for DomainJsonSchemas<'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let package_id = self.package_id;
        let next_package_id = PackageId(self.package_id.0 + 1);
        let mut map = serializer.serialize_map(None)?;

        // serialize schemas belonging to the domain package first
        for (_, operator_id) in self
            .schema_graph
            .range(DefId(package_id, 0)..DefId(next_package_id, 0))
        {
            map.serialize_entry("key", &new_schema(self.env, *operator_id, None))?;
        }

        for (def_id, operator_id) in &self.schema_graph {
            if def_id.0 != self.package_id {
                map.serialize_entry("key", &new_schema(self.env, *operator_id, None))?;
            }
        }

        map.end()
    }
}

fn new_schema<'e>(
    env: &'e Env,
    operator_id: SerdeOperatorId,
    rel_params_operator_id: Option<SerdeOperatorId>,
) -> JsonSchema<'e> {
    let value_operator = &env.serde_operators[operator_id.0 as usize];
    let rel_params_operator = rel_params_operator_id.map(|id| &env.serde_operators[id.0 as usize]);

    JsonSchema {
        env,
        value_operator,
        rel_params_operator,
    }
}

fn new_schema_link<'e>(env: &'e Env, operator_id: SerdeOperatorId) -> JsonSchema<'e> {
    let value_operator = &env.serde_operators[operator_id.0 as usize];

    JsonSchema {
        env,
        value_operator,
        rel_params_operator: None,
    }
}

struct JsonSchema<'e> {
    env: &'e Env,
    value_operator: &'e SerdeOperator,
    rel_params_operator: Option<&'e SerdeOperator>,
}

impl<'e> Serialize for JsonSchema<'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.value_operator {
            SerdeOperator::Unit => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "object")?;
                map.end()
            }
            // FIXME: Distinguish different number types
            SerdeOperator::Int(_) | SerdeOperator::Number(_) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "integer")?;
                map.serialize_entry("format", "int64")?;
                map.end()
            }
            SerdeOperator::String(_) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "string")?;
                map.end()
            }
            SerdeOperator::StringConstant(literal, _) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("enum", &[literal])?;
                map.end()
            }
            SerdeOperator::StringPattern(def_id)
            | SerdeOperator::CapturingStringPattern(def_id) => {
                let pattern = self.env.string_patterns.get(def_id).unwrap();
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("pattern", pattern.regex.as_str())?;
                map.end()
            }
            SerdeOperator::Sequence(ranges, _) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "array")?;
                match ranges.len() {
                    0 => {
                        map.serialize_entry::<str, [&'static str]>("items", &[])?;
                    }
                    1 => {
                        let range = ranges.iter().next().unwrap();
                        match &range.finite_repetition {
                            Some(_) => map.serialize_entry(
                                "items",
                                &ArrayItemsSchema::new(self.env, ranges.as_slice()),
                            )?,
                            None => {
                                map.serialize_entry(
                                    "items",
                                    &new_schema_link(self.env, range.operator_id),
                                )?;
                            }
                        }
                    }
                    len => {
                        let last_range = ranges.last().unwrap();
                        if last_range.finite_repetition.is_some() {
                            map.serialize_entry(
                                "items",
                                &ArrayItemsSchema::new(self.env, ranges.as_slice()),
                            )?;
                        } else {
                            map.serialize_entry(
                                "items",
                                &ArrayItemsSchema::new(self.env, &ranges[..len - 1]),
                            )?;
                            map.serialize_entry(
                                "additionalItems",
                                &new_schema_link(self.env, last_range.operator_id),
                            )?;
                        }
                    }
                }
                map.end()
            }
            SerdeOperator::ValueType(value_type) => todo!(),
            SerdeOperator::ValueUnionType(value_union_type) => {
                todo!()
            }
            SerdeOperator::Id(inner_operator_id) => {
                todo!()
            }
            SerdeOperator::MapType(map_type) => {
                todo!()
            }
        }
    }
}

struct ArrayItemsSchema<'e> {
    env: &'e Env,
    ranges: &'e [SequenceRange],
}

impl<'e> ArrayItemsSchema<'e> {
    fn new(env: &'e Env, ranges: &'e [SequenceRange]) -> Self {
        Self { env, ranges }
    }
}

impl<'e> Serialize for ArrayItemsSchema<'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        for range in self.ranges {
            if let Some(repetition) = range.finite_repetition {
                for _ in 0..repetition {
                    seq.serialize_element(&new_schema_link(&self.env, range.operator_id))?;
                }
            }
        }
        seq.end()
    }
}

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
