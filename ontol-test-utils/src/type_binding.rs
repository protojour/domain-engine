use fnv::FnvHashMap;
use jsonschema::JSONSchema;
use ontol_faker::new_constant_fake;
use ontol_runtime::{
    interface::json_schema::build_standalone_schema,
    interface::serde::operator::SerdeOperatorId,
    interface::serde::processor::{ProcessorLevel, ProcessorMode, DOMAIN_PROFILE},
    ontology::{Ontology, TypeInfo},
    select::{Select, StructSelect},
    value::{Attribute, Data, PropertyId, Value},
    DefId, PackageId,
};
use serde::de::DeserializeSeed;
use tracing::{debug, trace, warn};

use crate::{serde_utils::create_de, OntolTest};

/// This test asserts that JSON schemas accept the same things that
/// ONTOL's own deserializer does.
pub(crate) const TEST_JSON_SCHEMA_VALIDATION: bool = true;

pub struct TypeBinding<'on> {
    pub type_info: TypeInfo,
    json_schema: Option<JSONSchema>,
    ontology: &'on Ontology,
}

impl<'on> TypeBinding<'on> {
    pub(crate) fn new(ontol_test: &'on OntolTest, type_name: &str) -> Self {
        let (package_id, type_name) = ontol_test.parse_test_ident(type_name);
        Self::new_with_package(ontol_test, package_id, type_name)
    }

    fn new_with_package(
        ontol_test: &'on OntolTest,
        package_id: PackageId,
        type_name: &str,
    ) -> Self {
        let ontology = &ontol_test.ontology;
        let domain = ontology.find_domain(package_id).unwrap();
        let def_id = domain
            .type_names
            .get(type_name)
            .unwrap_or_else(|| panic!("type name not found: `{type_name}`"));
        let type_info = domain.type_info(*def_id).clone();

        if !type_info.public {
            warn!("`{:?}` is not public!", type_info.name);
        }

        trace!(
            "TypeBinding::new `{type_name}` with {operator_id:?} create={processor:?}",
            operator_id = type_info.operator_id,
            processor = type_info.operator_id.map(|id| ontology.new_serde_processor(
                id,
                ProcessorMode::Create,
                ProcessorLevel::new_root(),
                &DOMAIN_PROFILE
            ))
        );

        let json_schema = if ontol_test.compile_json_schema {
            Some(compile_json_schema(ontology, &type_info))
        } else {
            None
        };

        Self {
            type_info,
            json_schema,
            ontology,
        }
    }

    pub fn from_def_id(def_id: DefId, ontology: &'on Ontology) -> Self {
        Self {
            type_info: ontology.get_type_info(def_id).clone(),
            json_schema: None,
            ontology,
        }
    }

    pub fn ontology(&self) -> &Ontology {
        self.ontology
    }

    pub fn value_builder(&self, data: serde_json::Value) -> ValueBuilder<'_, 'on> {
        ValueBuilder {
            binding: self,
            value: Value::unit(),
        }
        .data(data)
    }

    pub fn new_fake(&self, processor_mode: ProcessorMode) -> Value {
        new_constant_fake(self.ontology, self.type_info.def_id, processor_mode).unwrap()
    }

    pub fn entity_builder(
        &self,
        id: serde_json::Value,
        data: serde_json::Value,
    ) -> ValueBuilder<'_, 'on> {
        ValueBuilder {
            binding: self,
            value: Value::unit(),
        }
        .id(id)
        .data(data)
    }

    pub fn def_id(&self) -> DefId {
        self.type_info.def_id
    }

    pub fn struct_select(
        &self,
        properties: impl IntoIterator<Item = (&'static str, Select)>,
    ) -> StructSelect {
        StructSelect {
            def_id: self.type_info.def_id,
            properties: FnvHashMap::from_iter(
                properties
                    .into_iter()
                    .map(|(prop_name, query)| (self.find_property(prop_name).unwrap(), query)),
            ),
        }
    }

    pub fn serde_operator_id(&self) -> SerdeOperatorId {
        self.type_info.operator_id.expect("No serde operator id")
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.ontology
            .new_serde_processor(
                self.serde_operator_id(),
                ProcessorMode::Create,
                ProcessorLevel::new_root(),
                &DOMAIN_PROFILE,
            )
            .find_property(prop)
    }

    pub fn json_schema(&self) -> Option<&JSONSchema> {
        self.json_schema.as_ref()
    }

    pub fn new_json_schema(&self, processor_mode: ProcessorMode) -> serde_json::Value {
        let schema =
            build_standalone_schema(self.ontology, &self.type_info, processor_mode).unwrap();
        serde_json::to_value(schema).unwrap()
    }
}

fn compile_json_schema(ontology: &Ontology, type_info: &TypeInfo) -> JSONSchema {
    let standalone_schema =
        build_standalone_schema(ontology, type_info, ProcessorMode::Create).unwrap();

    debug!(
        "outputted json schema: {}",
        serde_json::to_string_pretty(&standalone_schema).unwrap()
    );

    JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .compile(&serde_json::to_value(&standalone_schema).unwrap())
        .unwrap()
}

#[derive(Clone)]
pub struct ValueBuilder<'t, 'on> {
    binding: &'t TypeBinding<'on>,
    value: Value,
}

impl<'t, 'on> From<ValueBuilder<'t, 'on>> for Value {
    fn from(b: ValueBuilder<'t, 'on>) -> Self {
        b.value
    }
}

impl<'t, 'on> From<ValueBuilder<'t, 'on>> for Attribute {
    fn from(b: ValueBuilder<'t, 'on>) -> Attribute {
        b.to_unit_attr()
    }
}

impl<'t, 'on> ValueBuilder<'t, 'on> {
    pub fn relationship(self, name: &str, attribute: Attribute) -> Self {
        let property_id = self.binding.find_property(name).expect("unknown property");
        self.merge_attribute(property_id, attribute)
    }

    pub fn to_unit_attr(self) -> Attribute {
        self.value.to_unit_attr()
    }

    pub fn to_attr(self, rel_params: impl Into<Value>) -> Attribute {
        self.value.to_attr(rel_params.into())
    }

    fn data(mut self, json: serde_json::Value) -> Self {
        let value = create_de(self.binding).value(json).unwrap();
        self.value.type_def_id = value.type_def_id;
        match (&mut self.value.data, value) {
            (Data::Unit, value) => {
                self.value = value;
            }
            (
                Data::Struct(attrs_a),
                Value {
                    data: Data::Struct(attrs_b),
                    ..
                },
            ) => {
                attrs_a.extend(attrs_b);
            }
            (a, b) => panic!("Unable to merge {a:?} and {b:?}"),
        }
        self
    }

    fn id(self, json: serde_json::Value) -> Self {
        let entity_info = self
            .binding
            .type_info
            .entity_info
            .as_ref()
            .expect("Not an entity!");
        let id = self
            .binding
            .ontology
            .new_serde_processor(
                entity_info.id_operator_id,
                ProcessorMode::Create,
                ProcessorLevel::new_root(),
                &DOMAIN_PROFILE,
            )
            .deserialize(&mut serde_json::Deserializer::from_str(
                &serde_json::to_string(&json).unwrap(),
            ))
            .unwrap();

        self.merge_attribute(PropertyId::subject(entity_info.id_relationship_id), id)
    }

    fn merge_attribute(mut self, property_id: PropertyId, attribute: Attribute) -> Self {
        match &mut self.value.data {
            Data::Struct(attrs) => {
                attrs.insert(property_id, attribute);
            }
            Data::Unit => self.value.data = Data::Struct([(property_id, attribute)].into()),
            other => {
                panic!("Value data was not a map/unit, but {other:?}.")
            }
        }
        self
    }
}

pub trait ToSequence {
    fn to_sequence_attribute(self, ty: &TypeBinding) -> Attribute;
}

impl ToSequence for Vec<Attribute> {
    fn to_sequence_attribute(self, ty: &TypeBinding) -> Attribute {
        Value {
            data: Data::Sequence(self),
            type_def_id: ty.type_info.def_id,
        }
        .to_attr(Value::unit())
    }
}
