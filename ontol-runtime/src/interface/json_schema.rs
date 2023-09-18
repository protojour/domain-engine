use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;

use fnv::FnvHashSet;
use serde::Serialize;
use serde::{ser::SerializeMap, ser::SerializeSeq, Serializer};
use smartstring::alias::String;
use urlencoding::encode;

use crate::interface::serde::operator::{
    SequenceRange, SerdeOperator, SerdeOperatorId, StructOperator, UnionOperator,
};
use crate::interface::serde::processor::ProcessorMode;
use crate::ontology::TypeInfo;
use crate::smart_format;
use crate::{
    ontology::{Domain, Ontology},
    DefId, PackageId,
};

use super::serde::{SerdeDef, SerdeModifier};

pub fn build_openapi_schemas<'e>(
    ontology: &'e Ontology,
    package_id: PackageId,
    domain: &'e Domain,
) -> OpenApiSchemas<'e> {
    let mut graph_builder = SchemaGraphBuilder::default();

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);
        if let Some(operator_id) = &type_info.operator_id {
            graph_builder.visit(*operator_id, ontology);
        }
    }

    OpenApiSchemas {
        schema_graph: graph_builder.graph,
        package_id,
        ontology,
    }
}

pub fn build_standalone_schema<'e>(
    ontology: &'e Ontology,
    type_info: &TypeInfo,
    mode: ProcessorMode,
) -> Result<StandaloneJsonSchema<'e>, &'static str> {
    let mut graph_builder = SchemaGraphBuilder::default();

    let operator_id = type_info
        .operator_id
        .ok_or("no serde operator id available")?;
    graph_builder.visit(operator_id, ontology);

    Ok(StandaloneJsonSchema {
        operator_id,
        def_id: type_info.def_id,
        defs: graph_builder.graph,
        ontology,
        mode,
    })
}

type DefMap = BTreeMap<SerdeDef, SerdeOperatorId>;

/// Includes all the schemas in a domain, for use in OpenApi
pub struct OpenApiSchemas<'e> {
    schema_graph: BTreeMap<SerdeDef, SerdeOperatorId>,
    package_id: PackageId,
    ontology: &'e Ontology,
}

impl<'e> Serialize for OpenApiSchemas<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let package_id = self.package_id;
        let next_package_id = PackageId(self.package_id.0 + 1);
        let mut map = serializer.serialize_map(None)?;

        let ctx = SchemaCtx {
            link_anchor: LinkAnchor::ComponentsSchemas,
            ontology: self.ontology,
            docs: None,
            rel_params_operator_id: None,
            mode: ProcessorMode::Create,
        };

        // serialize schema definitions belonging to the domain package first
        for (def_id, operator_id) in self.schema_graph.range(
            SerdeDef::new(DefId(package_id, 0), SerdeModifier::NONE)
                ..SerdeDef::new(DefId(next_package_id, 0), SerdeModifier::NONE),
        ) {
            map.serialize_entry(&ctx.format_key(*def_id), &ctx.definition(*operator_id))?;
        }

        for (serde_def, operator_id) in &self.schema_graph {
            if serde_def.def_id.0 != self.package_id {
                map.serialize_entry(&ctx.format_key(*serde_def), &ctx.definition(*operator_id))?;
            }
        }

        map.end()
    }
}

/// Schema for a single type, for use in JSON schema
pub struct StandaloneJsonSchema<'e> {
    operator_id: SerdeOperatorId,
    def_id: DefId,
    defs: BTreeMap<SerdeDef, SerdeOperatorId>,
    ontology: &'e Ontology,
    mode: ProcessorMode,
}

impl<'e> Serialize for StandaloneJsonSchema<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let docs = self.ontology.get_docs(self.def_id);
        let ctx = SchemaCtx {
            link_anchor: LinkAnchor::Defs,
            ontology: self.ontology,
            docs: docs.as_deref(),
            rel_params_operator_id: None,
            mode: self.mode,
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
    ontology: &'e Ontology,
    docs: Option<&'e str>,
    rel_params_operator_id: Option<SerdeOperatorId>,
    mode: ProcessorMode,
}

impl<'e> SchemaCtx<'e> {
    fn definition(self, operator_id: SerdeOperatorId) -> SchemaDefinition<'e> {
        SchemaDefinition {
            ctx: self,
            value_operator: self.ontology.get_serde_operator(operator_id),
        }
    }

    fn reference(&self, link: SerdeOperatorId) -> SchemaReference<'static, 'e> {
        SchemaReference {
            ctx: *self,
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
    fn ref_link(&self, serde_def: SerdeDef) -> RefLink {
        RefLink {
            ctx: *self,
            serde_def,
        }
    }

    fn with_rel_params(&self, rel_params_operator_id: Option<SerdeOperatorId>) -> Self {
        Self {
            link_anchor: self.link_anchor,
            ontology: self.ontology,
            rel_params_operator_id,
            docs: self.docs,
            mode: self.mode,
        }
    }

    fn with_docs(&self, docs: Option<&'e str>) -> Self {
        Self {
            link_anchor: self.link_anchor,
            ontology: self.ontology,
            rel_params_operator_id: self.rel_params_operator_id,
            docs,
            mode: self.mode,
        }
    }

    fn type_name(&self, serde_def: SerdeDef) -> Option<String> {
        let mut modifier = String::from("");
        if serde_def.modifier.contains(SerdeModifier::PRIMARY_ID) {
            modifier.push_str("_id");
        }
        if serde_def.modifier.contains(SerdeModifier::UNION) {
            modifier.push_str("_union");
        }
        if serde_def.modifier.contains(SerdeModifier::ARRAY) {
            modifier.push_str("_array");
        }
        self.ontology.find_domain(serde_def.def_id.0).map(|domain| {
            smart_format!(
                "{}{}",
                domain.type_info(serde_def.def_id).name.as_ref().unwrap(),
                modifier
            )
        })
    }

    fn format_key(&self, serde_def: SerdeDef) -> String {
        let package_id = serde_def.def_id.0;
        match self.type_name(serde_def) {
            Some(name) => smart_format!("{}_{}", package_id.0, encode(&name)),
            None => smart_format!("{}", Key(serde_def)),
        }
    }

    fn format_ref_link(&self, serde_def: SerdeDef) -> String {
        let package_id = serde_def.def_id.0;
        match self.type_name(serde_def) {
            Some(name) => smart_format!("{}{}_{}", self.link_anchor, package_id.0, encode(&name)),
            None => smart_format!("{}{}", self.link_anchor, Key(serde_def)),
        }
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

struct Key(SerdeDef);

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = &self.0;
        let package = self.0.def_id.0;

        write!(f, "{}_{}", package.0, self.0.def_id.1)?;

        if variant.modifier.contains(SerdeModifier::PRIMARY_ID) {
            write!(f, "_id")?;
        }
        if variant.modifier.contains(SerdeModifier::UNION) {
            write!(f, "_union")?;
        }
        if variant.modifier.contains(SerdeModifier::ARRAY) {
            write!(f, "_array")?;
        }

        Ok(())
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
        serialize_schema_inline::<S>(&self.ctx, self.value_operator, None, &mut map)?;
        map.end()
    }
}

fn serialize_schema_inline<S: Serializer>(
    ctx: &SchemaCtx,
    value_operator: &SerdeOperator,
    def_map: Option<&DefMap>,
    map: &mut <S as Serializer>::SerializeMap,
) -> Result<(), S::Error> {
    match value_operator {
        SerdeOperator::Unit => {
            map.serialize_entry("type", "object")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::True(_) => {
            map.serialize_entry("type", "boolean")?;
            map.serialize_entry("enum", &[true])?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::False(_) => {
            map.serialize_entry("type", "boolean")?;
            map.serialize_entry("enum", &[false])?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::Boolean(_) => {
            map.serialize_entry("type", "boolean")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        // FIXME: Distinguish different number types
        SerdeOperator::I64(..) | SerdeOperator::F64(..) => {
            map.serialize_entry("type", "integer")?;
            map.serialize_entry("format", "int64")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::String(_) => {
            map.serialize_entry("type", "string")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::StringConstant(literal, _) => {
            map.serialize_entry("type", "string")?;
            map.serialize_entry("enum", &[literal])?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::TextPattern(def_id) | SerdeOperator::CapturingTextPattern(def_id) => {
            let pattern = ctx.ontology.text_patterns.get(def_id).unwrap();
            map.serialize_entry("type", "string")?;
            map.serialize_entry("pattern", pattern.regex.as_str())?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::DynamicSequence => {
            map.serialize_entry("type", "array")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
        }
        SerdeOperator::RelationSequence(seq_op) => {
            map.serialize_entry("type", "array")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }

            let range = &seq_op.ranges[0];
            // normal array: all items are uniform
            map.serialize_entry("items", &ctx.reference(range.operator_id))?;
        }
        SerdeOperator::ConstructorSequence(seq_op) => {
            map.serialize_entry("type", "array")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }

            let ranges = &seq_op.ranges;

            if ranges.is_empty() {
                map.serialize_entry::<str, [&'static str]>("items", &[])?;
            } else {
                let len = ranges.len();
                let last_range = ranges.last().unwrap();
                let len_range = seq_op.length_range();

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
        SerdeOperator::Alias(value_op) => {
            serialize_schema_inline::<S>(
                ctx,
                ctx.ontology.get_serde_operator(value_op.inner_operator_id),
                None,
                map,
            )?;
        }
        SerdeOperator::Union(union_op) => {
            map.serialize_entry(
                "oneOf",
                &UnionRefLinks {
                    ctx: *ctx,
                    union_op,
                },
            )?;
        }
        SerdeOperator::IdSingletonStruct(_name, _inner_operator_id) => {
            panic!("BUG: Id not handled here")
        }
        SerdeOperator::Struct(struct_op) => {
            map.serialize_entry("type", "object")?;
            if let Some(docs) = ctx.docs {
                map.serialize_entry("description", docs)?;
            }
            map.serialize_entry(
                "properties",
                &MapProperties {
                    ctx: ctx.with_rel_params(None),
                    map_type: struct_op,
                },
            )?;

            let required_count = struct_op.required_count(ctx.mode, None);
            if required_count > 0 {
                map.serialize_entry(
                    "required",
                    &RequiredMapProperties {
                        map_type: struct_op,
                    },
                )?;
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
        let value_operator = self.ctx.ontology.get_serde_operator(self.operator_id);

        match value_operator {
            SerdeOperator::Unit
            | SerdeOperator::False(_)
            | SerdeOperator::True(_)
            | SerdeOperator::Boolean(_)
            | SerdeOperator::I64(..)
            | SerdeOperator::F64(..)
            | SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::TextPattern(_)
            | SerdeOperator::CapturingTextPattern(_)
            | SerdeOperator::DynamicSequence => {
                // These are inline schemas
                let mut map = serializer.serialize_map(None)?;
                serialize_schema_inline::<S>(&self.ctx, value_operator, self.def_map, &mut map)?;
                map.end()
            }
            SerdeOperator::RelationSequence(_seq_op) => {
                let mut map = serializer.serialize_map(None)?;
                serialize_schema_inline::<S>(&self.ctx, value_operator, self.def_map, &mut map)?;
                map.end()
            }
            SerdeOperator::ConstructorSequence(seq_op) => self
                .compose(self.ctx.ref_link(seq_op.def))
                .serialize(serializer),
            SerdeOperator::Alias(value_op) => self
                .compose(self.ctx.ref_link(value_op.def))
                .serialize(serializer),
            SerdeOperator::Union(union_op) => self
                .compose(self.ctx.ref_link(union_op.union_def()))
                .serialize(serializer),
            SerdeOperator::IdSingletonStruct(name, id_operator_id) => self
                .compose(self.ctx.singleton_object(name.as_str(), *id_operator_id))
                .serialize(serializer),
            SerdeOperator::Struct(struct_op) => self
                .compose(self.ctx.ref_link(struct_op.def))
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
        if let Some(rel_params_operator_id) = self.ctx.rel_params_operator_id {
            let mut map = serializer.serialize_map(Some(1))?;

            // Remove the rel params to avoid infinite recursion!
            let ctx = self.ctx.with_rel_params(None);

            map.serialize_entry(
                "allOf",
                // note: serializing a tuple results in a sequence:
                &(
                    // This reference should eventually hit the else clause below, because new ctx has no rel_params:
                    ctx.reference(self.operator_id),
                    ctx.singleton_object("_edge", rel_params_operator_id),
                ),
            )?;

            // FIXME: This is not validated in jsonschema-rs (https://github.com/Stranger6667/jsonschema-rs/issues/288)
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
    def_map: Option<&'d BTreeMap<SerdeDef, SerdeOperatorId>>,
}

impl<'d, 'e, T: Serialize> Serialize for ClosedMap<'d, 'e, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("allOf", &[&self.inner])?;
        map.serialize_entry("unevaluatedProperties", &false)?;

        Defs::serialize_if_present::<S>(&self.ctx, &self.def_map, &mut map)?;

        map.end()
    }
}

struct UnionRefLinks<'e> {
    ctx: SchemaCtx<'e>,
    union_op: &'e UnionOperator,
}

impl<'e> Serialize for UnionRefLinks<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;

        for discriminator in self
            .union_op
            // BUG: Must filter variants
            .unfiltered_variants()
        {
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
    map_type: &'e StructOperator,
}

impl<'e> Serialize for MapProperties<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (key, property) in &self.map_type.properties {
            let docs = self
                .ctx
                .ontology
                .get_docs(property.property_id.relationship_id.0);
            map.serialize_entry(
                key,
                &self
                    .ctx
                    .with_rel_params(property.rel_params_operator_id)
                    .with_docs(docs.as_deref())
                    .reference(property.value_operator_id),
            )?;
        }
        map.end()
    }
}

// ["a", "b"] in { "type": "object", "required": _ }
struct RequiredMapProperties<'e> {
    map_type: &'e StructOperator,
}

impl<'e> Serialize for RequiredMapProperties<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for (key, property) in &self.map_type.properties {
            if !property.is_optional() {
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
    serde_def: SerdeDef,
}

impl<'e> Serialize for RefLink<'e> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("$ref", &self.ctx.format_ref_link(self.serde_def))?;
        map.end()
    }
}

// { "$defs": { .. } }
struct Defs<'d, 'e> {
    ctx: SchemaCtx<'e>,
    def_map: &'d BTreeMap<SerdeDef, SerdeOperatorId>,
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
    graph: BTreeMap<SerdeDef, SerdeOperatorId>,
    visited: FnvHashSet<SerdeOperatorId>,
}

impl SchemaGraphBuilder {
    fn visit(&mut self, operator_id: SerdeOperatorId, ontology: &Ontology) {
        if !self.mark_visited(operator_id) {
            return;
        }

        let operator = ontology.get_serde_operator(operator_id);

        match operator {
            SerdeOperator::Unit
            | SerdeOperator::False(_)
            | SerdeOperator::True(_)
            | SerdeOperator::Boolean(_)
            | SerdeOperator::I64(..)
            | SerdeOperator::F64(..)
            | SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::TextPattern(_)
            | SerdeOperator::CapturingTextPattern(_)
            | SerdeOperator::DynamicSequence => {}
            SerdeOperator::ConstructorSequence(seq_op) => {
                self.add_to_graph(seq_op.def, operator_id);
                for range in &seq_op.ranges {
                    self.visit(range.operator_id, ontology);
                }
            }
            SerdeOperator::RelationSequence(seq_op) => {
                for range in &seq_op.ranges {
                    self.visit(range.operator_id, ontology);
                }
            }
            SerdeOperator::Alias(value_op) => {
                self.add_to_graph(value_op.def, operator_id);
                self.visit(value_op.inner_operator_id, ontology);
            }
            SerdeOperator::Union(union_op) => {
                self.add_to_graph(union_op.union_def(), operator_id);

                for discriminator in union_op.unfiltered_variants() {
                    self.visit(discriminator.operator_id, ontology);
                }
            }
            SerdeOperator::IdSingletonStruct(_, id_operator_id) => {
                // id is not represented in the graph
                self.visit(*id_operator_id, ontology);
            }
            SerdeOperator::Struct(struct_op) => {
                self.add_to_graph(struct_op.def, operator_id);

                for (_, property) in &struct_op.properties {
                    self.visit(property.value_operator_id, ontology);
                    if let Some(operator_id) = &property.rel_params_operator_id {
                        self.visit(*operator_id, ontology);
                    }
                }
            }
        }
    }

    fn add_to_graph(&mut self, serde_def: SerdeDef, operator_id: SerdeOperatorId) {
        if self.graph.insert(serde_def, operator_id).is_some() {
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
