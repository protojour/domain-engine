use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;

use fnv::FnvHashSet;
use serde::Serialize;
use serde::{ser::SerializeMap, ser::SerializeSeq, Serializer};
use smartstring::alias::String;
use tracing::debug;

use crate::env::TypeInfo;
use crate::serde::{MapType, SequenceRange, ValueUnionType};
use crate::{
    env::{Domain, Env},
    serde::{SerdeOperator, SerdeOperatorId},
    DefId, PackageId,
};
use crate::{smart_format, DataVariant, DefVariant};

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

    Ok(StandaloneJsonSchema {
        operator_id,
        defs: graph_builder.graph,
        env,
    })
}

type DefMap = BTreeMap<DefVariant, SerdeOperatorId>;

/// Includes all the schemas in a domain, for use in OpenApi
pub struct OpenApiSchemas<'e> {
    schema_graph: BTreeMap<DefVariant, SerdeOperatorId>,
    package_id: PackageId,
    env: &'e Env,
}

impl<'e> Serialize for OpenApiSchemas<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let package_id = self.package_id;
        let next_package_id = PackageId(self.package_id.0 + 1);
        let mut map = serializer.serialize_map(None)?;

        let ctx = SchemaCtx {
            link_anchor: LinkAnchor::ComponentsSchemas,
            env: self.env,
            rel_params_operator_id: None,
            allof_reference_depth: 0,
        };

        // serialize schema definitions belonging to the domain package first
        for (def_id, operator_id) in self.schema_graph.range(
            DefVariant::identity(DefId(package_id, 0))
                ..DefVariant::identity(DefId(next_package_id, 0)),
        ) {
            map.serialize_entry(&ctx.format_key(*def_id), &ctx.definition(*operator_id))?;
        }

        for (def_variant, operator_id) in &self.schema_graph {
            if def_variant.id().0 != self.package_id {
                map.serialize_entry(&ctx.format_key(*def_variant), &ctx.definition(*operator_id))?;
            }
        }

        map.end()
    }
}

/// Schema for a single type, for use in JSON schema
pub struct StandaloneJsonSchema<'e> {
    operator_id: SerdeOperatorId,
    defs: BTreeMap<DefVariant, SerdeOperatorId>,
    env: &'e Env,
}

impl<'e> Serialize for StandaloneJsonSchema<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let ctx = SchemaCtx {
            link_anchor: LinkAnchor::Defs,
            env: self.env,
            rel_params_operator_id: None,
            allof_reference_depth: 0,
        };

        SchemaReference {
            ctx,
            operator_id: self.operator_id,
            def_map: if self.defs.is_empty() {
                None
            } else {
                Some(&self.defs)
            },
        }
        .serialize(serializer)
    }
}

#[derive(Clone, Copy)]
struct SchemaCtx<'e> {
    link_anchor: LinkAnchor,
    env: &'e Env,
    rel_params_operator_id: Option<SerdeOperatorId>,
    allof_reference_depth: u8,
}

impl<'e> SchemaCtx<'e> {
    fn definition(self, operator_id: SerdeOperatorId) -> SchemaDefinition<'e> {
        SchemaDefinition {
            ctx: self,
            value_operator: self.env.get_serde_operator(operator_id),
        }
    }

    fn reference(&self, link: SerdeOperatorId) -> SchemaReference<'static, 'e> {
        SchemaReference {
            ctx: *self,
            operator_id: link,
            def_map: None,
        }
    }

    fn allof_reference(&self, link: SerdeOperatorId) -> SchemaReference<'static, 'e> {
        SchemaReference {
            ctx: Self {
                link_anchor: self.link_anchor,
                env: self.env,
                rel_params_operator_id: self.rel_params_operator_id,
                allof_reference_depth: self.allof_reference_depth + 1,
            },
            operator_id: link,
            def_map: None,
        }
    }

    fn items_ref_links(&self, ranges: &'e [SequenceRange]) -> ArrayItemsRefLinks<'e> {
        ArrayItemsRefLinks { ctx: *self, ranges }
    }

    fn singleton_object(
        &self,
        property_name: impl Into<String>,
        operator_id: SerdeOperatorId,
    ) -> SingletonObjectSchema<'e> {
        SingletonObjectSchema {
            ctx: *self,
            property_name: property_name.into(),
            operator_id,
        }
    }

    /// Returns something that serializes as `{ "$ref": "some link" }`
    fn ref_link(&self, def_variant: DefVariant) -> RefLink {
        RefLink {
            ctx: *self,
            def_variant,
        }
    }

    fn with_rel_params(&self, rel_params_operator_id: Option<SerdeOperatorId>) -> Self {
        Self {
            link_anchor: self.link_anchor,
            env: self.env,
            rel_params_operator_id,
            allof_reference_depth: self.allof_reference_depth,
        }
    }

    fn format_key(&self, def_variant: DefVariant) -> String {
        smart_format!("{}", Key(def_variant))
    }

    fn format_ref_link(&self, def_variant: DefVariant) -> String {
        smart_format!("{}{}", self.link_anchor, Key(def_variant))
    }
}

#[derive(Clone, Copy)]
enum LinkAnchor {
    /// A JSON schema for a specific def variant
    Defs,
    /// The #/components/schemas location inside an OpenAPI document
    ComponentsSchemas,
}

impl Display for LinkAnchor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkAnchor::Defs => {
                write!(f, "#/$defs/")
            }
            LinkAnchor::ComponentsSchemas => {
                write!(f, "#/components/schemas/")
            }
        }
    }
}

struct Key(DefVariant);

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = &self.0;
        let def_id = variant.id();
        let package = def_id.0;

        write!(
            f,
            "{}_{}{}",
            package.0,
            def_id.1,
            match variant.data_variant() {
                DataVariant::Identity => "",
                DataVariant::Array => "_array",
                DataVariant::IdMap => "_id",
                DataVariant::InherentPropertyMap => "_inherent_props",
                DataVariant::JoinedPropertyMap => "_props",
            }
        )
    }
}

#[derive(Clone, Copy)]
struct SchemaDefinition<'e> {
    ctx: SchemaCtx<'e>,
    value_operator: &'e SerdeOperator,
}

impl<'e> Serialize for SchemaDefinition<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        serialize_schema_inline::<S>(&self.ctx, self.value_operator, None, None, &mut map)?;
        map.end()
    }
}

fn serialize_schema_inline<S: Serializer>(
    ctx: &SchemaCtx,
    value_operator: &SerdeOperator,
    _description: Option<&str>,
    def_map: Option<&DefMap>,
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
        SerdeOperator::Sequence(sequence_type) => {
            map.serialize_entry("type", "array")?;

            let ranges = &sequence_type.ranges;

            if ranges.is_empty() {
                map.serialize_entry::<str, [&'static str]>("items", &[])?;
            } else {
                let len = ranges.len();
                let last_range = ranges.last().unwrap();
                let len_range = sequence_type.length_range();

                if last_range.finite_repetition.is_some() {
                    // a finite sequence/tuple
                    map.serialize_entry("prefixItems", &ctx.items_ref_links(ranges))?;
                    map.serialize_entry("items", &false)?;
                    if let Some(min) = len_range.start {
                        map.serialize_entry("minItems", &min)?;
                    }
                } else if len == 1 {
                    // normal array: all items are uniform
                    map.serialize_entry("items", &ctx.reference(last_range.operator_id))?;
                } else {
                    // tuple followed by array
                    map.serialize_entry("prefixItems", &ctx.items_ref_links(&ranges[..len - 1]))?;
                    map.serialize_entry("items", &ctx.reference(last_range.operator_id))?;
                    if let Some(min) = len_range.start {
                        map.serialize_entry("minItems", &min)?;
                    }
                }
            }
        }
        SerdeOperator::ValueType(value_type) => {
            serialize_schema_inline::<S>(
                ctx,
                ctx.env.get_serde_operator(value_type.inner_operator_id),
                Some("TODO: overridden description"),
                None,
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
                    ctx: ctx.with_rel_params(None),
                    map_type,
                },
            )?;
            if map_type.n_mandatory_properties > 0 {
                map.serialize_entry("required", &RequiredMapProperties { map_type })?;
            }
            // map.serialize_entry("additionalProperties", &false)?;
        }
    };

    Defs::serialize_if_present::<S>(ctx, &def_map, map)?;

    Ok(())
}

/// A reference to a schema, at use-site
#[derive(Clone, Copy)]
struct SchemaReference<'d, 'e> {
    ctx: SchemaCtx<'e>,
    operator_id: SerdeOperatorId,
    def_map: Option<&'d DefMap>,
}

impl<'d, 'e> Serialize for SchemaReference<'d, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value_operator = self.ctx.env.get_serde_operator(self.operator_id);

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
                serialize_schema_inline::<S>(
                    &self.ctx,
                    value_operator,
                    None,
                    self.def_map,
                    &mut map,
                )?;
                map.end()
            }
            SerdeOperator::Sequence(sequence_type) => {
                if sequence_type.is_constructor {
                    self.compose(self.ctx.ref_link(sequence_type.def_variant))
                        .serialize(serializer)
                } else {
                    let mut map = serializer.serialize_map(None)?;
                    serialize_schema_inline::<S>(
                        &self.ctx,
                        value_operator,
                        None,
                        self.def_map,
                        &mut map,
                    )?;
                    map.end()
                }
            }
            SerdeOperator::ValueType(value_type) => self
                .compose(self.ctx.ref_link(value_type.def_variant))
                .serialize(serializer),
            SerdeOperator::ValueUnionType(value_union_type) => self
                .compose(self.ctx.ref_link(value_union_type.union_def_variant))
                .serialize(serializer),
            SerdeOperator::Id(id_operator_id) => self
                .compose(self.ctx.singleton_object("_id", *id_operator_id))
                .serialize(serializer),
            SerdeOperator::MapType(map_type) => self
                .compose(self.ctx.ref_link(map_type.def_variant))
                .serialize(serializer),
        }
    }
}

impl<'d, 'e> SchemaReference<'d, 'e> {
    fn compose<T: Serialize>(&self, inner: T) -> Compose<T> {
        Compose {
            ctx: self.ctx,
            operator_id: self.operator_id,
            inner,
            def_map: self.def_map,
        }
    }
}

struct Compose<'d, 'e, T> {
    ctx: SchemaCtx<'e>,
    operator_id: SerdeOperatorId,
    inner: T,
    def_map: Option<&'d DefMap>,
}

impl<'d, 'e, T: Serialize> Serialize for Compose<'d, 'e, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        debug!("allof_reference_depth: {}", self.ctx.allof_reference_depth);

        if let Some(rel_params_operator_id) = self.ctx.rel_params_operator_id {
            let mut map = serializer.serialize_map(Some(1))?;

            // Remove the rel params to avoid infinite recursion!
            let ctx = self.ctx.with_rel_params(None);

            map.serialize_entry(
                "allOf",
                // note: serializing a tuple results in a sequence:
                &(
                    // This reference should eventually hit the else clause below, because new ctx has no rel_params:
                    ctx.allof_reference(self.operator_id),
                    ctx.singleton_object("_edge", rel_params_operator_id),
                ),
            )?;
            map.serialize_entry("properties", &HashMap::<String, String>::default())?;

            // FIXME: This is not working correctly
            map.serialize_entry("unevaluatedProperties", &false)?;

            Defs::serialize_if_present::<S>(&self.ctx, &self.def_map, &mut map)?;

            map.end()
        } else {
            ClosedMap {
                ctx: self.ctx,
                inner: &self.inner,
                def_map: self.def_map,
            }
            .serialize(serializer)
        }
    }
}

struct ClosedMap<'d, 'e, T> {
    ctx: SchemaCtx<'e>,
    inner: T,
    def_map: Option<&'d BTreeMap<DefVariant, SerdeOperatorId>>,
}

impl<'d, 'e, T: Serialize> Serialize for ClosedMap<'d, 'e, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("allOf", &[&self.inner])?;
        map.serialize_entry("properties", &HashMap::<String, String>::default())?;
        map.serialize_entry("unevaluatedProperties", &false)?;

        Defs::serialize_if_present::<S>(&self.ctx, &self.def_map, &mut map)?;

        map.end()
    }
}

struct UnionRefLinks<'e> {
    ctx: SchemaCtx<'e>,
    value_union_type: &'e ValueUnionType,
}

impl<'e> Serialize for UnionRefLinks<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;

        for discriminator in &self.value_union_type.discriminators {
            seq.serialize_element(&self.ctx.reference(discriminator.operator_id))?;
        }

        seq.end()
    }
}

struct ArrayItemsRefLinks<'e> {
    ctx: SchemaCtx<'e>,
    ranges: &'e [SequenceRange],
}

impl<'e> Serialize for ArrayItemsRefLinks<'e> {
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

// properties in { "type": "object", "properties": _ }
struct MapProperties<'e> {
    ctx: SchemaCtx<'e>,
    map_type: &'e MapType,
}

impl<'e> Serialize for MapProperties<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (key, property) in &self.map_type.properties {
            // FIXME: Need to merge the documentation of the _property_
            // and the documentation of the type

            map.serialize_entry(
                key,
                &self
                    .ctx
                    .with_rel_params(property.rel_params_operator_id)
                    .reference(property.value_operator_id),
            )?;
        }
        map.end()
    }
}

// ["a", "b"] in { "type": "object", "required": _ }
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

struct SingletonObjectSchema<'e> {
    ctx: SchemaCtx<'e>,
    property_name: String,
    operator_id: SerdeOperatorId,
}

impl<'e> Serialize for SingletonObjectSchema<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        let prop = self.property_name.as_str();

        let properties: HashMap<_, _> = [(prop, self.ctx.reference(self.operator_id))].into();

        map.serialize_entry("type", "object")?;
        map.serialize_entry("properties", &properties)?;
        map.serialize_entry("required", &[prop])?;

        map.end()
    }
}

// { "_ref": "some-link" }
struct RefLink<'e> {
    ctx: SchemaCtx<'e>,
    def_variant: DefVariant,
}

impl<'e> Serialize for RefLink<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("$ref", &self.ctx.format_ref_link(self.def_variant))?;
        map.end()
    }
}

// { "$defs": { .. } }
struct Defs<'d, 'e> {
    ctx: SchemaCtx<'e>,
    def_map: &'d BTreeMap<DefVariant, SerdeOperatorId>,
}

impl<'d, 'e> Defs<'d, 'e> {
    fn serialize_if_present<S: Serializer>(
        ctx: &SchemaCtx<'e>,
        def_map: &Option<&'d DefMap>,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        if let Some(def_map) = def_map {
            map.serialize_entry("$defs", &Self { ctx: *ctx, def_map })
        } else {
            Ok(())
        }
    }
}

impl<'d, 'e> Serialize for Defs<'d, 'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        for (def_id, operator_id) in self.def_map {
            map.serialize_entry(
                &self.ctx.format_key(*def_id),
                &self.ctx.definition(*operator_id),
            )?;
        }

        map.end()
    }
}

#[derive(Default)]
struct SchemaGraphBuilder {
    graph: BTreeMap<DefVariant, SerdeOperatorId>,
    visited: FnvHashSet<SerdeOperatorId>,
}

impl SchemaGraphBuilder {
    fn visit(&mut self, operator_id: SerdeOperatorId, env: &Env) {
        if !self.mark_visited(operator_id) {
            return;
        }

        let operator = env.get_serde_operator(operator_id);

        match operator {
            SerdeOperator::Unit
            | SerdeOperator::Int(_)
            | SerdeOperator::Number(_)
            | SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::StringPattern(_)
            | SerdeOperator::CapturingStringPattern(_) => {}
            SerdeOperator::Sequence(sequence_type) => {
                if sequence_type.is_constructor {
                    self.add_to_graph(sequence_type.def_variant, operator_id);
                }

                for range in &sequence_type.ranges {
                    self.visit(range.operator_id, env);
                }
            }
            SerdeOperator::ValueType(value_type) => {
                self.add_to_graph(value_type.def_variant, operator_id);
                self.visit(value_type.inner_operator_id, env);
            }
            SerdeOperator::ValueUnionType(value_union_type) => {
                self.add_to_graph(value_union_type.union_def_variant, operator_id);

                for discriminator in &value_union_type.discriminators {
                    self.visit(discriminator.operator_id, env);
                }
            }
            SerdeOperator::Id(id_operator_id) => {
                // id is not represented in the graph
                self.visit(*id_operator_id, env);
            }
            SerdeOperator::MapType(map_type) => {
                self.add_to_graph(map_type.def_variant, operator_id);

                for (_, property) in &map_type.properties {
                    self.visit(property.value_operator_id, env);
                    if let Some(operator_id) = &property.rel_params_operator_id {
                        self.visit(*operator_id, env);
                    }
                }
            }
        }
    }

    fn add_to_graph(&mut self, def_variant: DefVariant, operator_id: SerdeOperatorId) {
        if self.graph.insert(def_variant, operator_id).is_some() {
            // panic!("{def_id:?} was already in graph");
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
