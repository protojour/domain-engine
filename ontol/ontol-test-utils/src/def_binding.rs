use std::collections::BTreeMap;

use ontol_core::tag::DomainIndex;
use ontol_faker::new_constant_fake;
use ontol_runtime::{
    DefId, PropId,
    attr::Attr,
    debug::OntolDebug,
    interface::{
        json_schema::build_standalone_schema,
        serde::{
            deserialize_raw,
            operator::SerdeOperatorAddr,
            processor::{ProcessorLevel, ProcessorMode},
        },
    },
    ontology::{Ontology, domain::Def},
    query::select::{Select, StructSelect},
    tuple::EndoTuple,
    value::Value,
};
use serde::de::DeserializeSeed;
use tracing::{trace, warn};

use crate::{OntolTest, serde_helper::serde_create};

pub struct DefBinding<'on> {
    pub def: &'on Def,
    ontology: &'on Ontology,
    #[cfg(feature = "validate-jsonschema")]
    json_schema: Option<jsonschema::Validator>,
}

impl<'on> DefBinding<'on> {
    pub(crate) fn new(ontol_test: &'on OntolTest, type_name: &str) -> Self {
        let (domain_index, type_name) = ontol_test.parse_test_ident(type_name);
        Self::new_with_domain(ontol_test, domain_index, type_name)
    }

    fn new_with_domain(
        ontol_test: &'on OntolTest,
        domain_index: DomainIndex,
        type_name: &str,
    ) -> Self {
        let ontology = ontol_test.ontology.as_ref();
        let domain = ontology.domain_by_index(domain_index).unwrap();
        let def = domain
            .defs()
            .find(|def| match def.ident().map(|name| &ontology[name]) {
                Some(name) => name == type_name,
                None => false,
            })
            .unwrap_or_else(|| panic!("type not found: `{type_name}`"));

        if !def.public {
            warn!("`{:?}` is not public!", def.ident().debug(ontology));
        }

        trace!(
            "TypeBinding::new `{type_name}` with {addr:?} create={processor:?}",
            addr = def.operator_addr,
            processor = def
                .operator_addr
                .map(|id| ontology.new_serde_processor(id, ProcessorMode::Create))
        );

        Self {
            def,
            ontology,
            #[cfg(feature = "validate-jsonschema")]
            json_schema: if ontol_test.compile_json_schema {
                Some(compile_json_schema(ontology, def))
            } else {
                None
            },
        }
    }

    pub fn from_def_id(def_id: DefId, ontology: &'on Ontology) -> Self {
        Self {
            def: ontology.def(def_id),
            #[cfg(feature = "validate-jsonschema")]
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
        .with_json_data(data)
    }

    pub fn new_fake(&self, processor_mode: ProcessorMode) -> Value {
        new_constant_fake(self.ontology, self.def.id, processor_mode).unwrap()
    }

    #[track_caller]
    pub fn entity_builder(
        &self,
        id: serde_json::Value,
        data: serde_json::Value,
    ) -> ValueBuilder<'_, 'on> {
        ValueBuilder {
            binding: self,
            value: Value::unit(),
        }
        .with_json_id(id)
        .with_json_data(data)
    }

    pub fn def_id(&self) -> DefId {
        self.def.id
    }

    pub fn graphql_def_id(&self) -> String {
        let domain = self
            .ontology
            .domain_by_index(self.def.id.domain_index())
            .unwrap();

        format!(
            "{}{}:{}",
            if self.def_id().is_persistent() {
                ""
            } else {
                "~"
            },
            domain.domain_id().id,
            self.def.id.tag()
        )
    }

    #[track_caller]
    pub fn entity_id_def_id(&self) -> DefId {
        self.def.entity().expect("not an entity").id_value_def_id
    }

    pub fn struct_select(
        &self,
        properties: impl IntoIterator<Item = (&'static str, Select)>,
    ) -> StructSelect {
        StructSelect {
            def_id: self.def.id,
            properties: BTreeMap::from_iter(
                properties
                    .into_iter()
                    .map(|(prop_name, query)| (self.find_property(prop_name).unwrap(), query)),
            ),
        }
    }

    pub fn serde_operator_addr(&self) -> SerdeOperatorAddr {
        self.def.operator_addr.expect("No serde operator addr")
    }

    pub fn find_property(&self, prop: &str) -> Option<PropId> {
        self.ontology
            .new_serde_processor(self.serde_operator_addr(), ProcessorMode::Create)
            .find_property(prop)
    }

    #[cfg(feature = "validate-jsonschema")]
    pub fn json_schema(&self) -> Option<&jsonschema::Validator> {
        self.json_schema.as_ref()
    }

    pub fn new_json_schema(&self, processor_mode: ProcessorMode) -> serde_json::Value {
        let schema = build_standalone_schema(self.ontology, self.def, processor_mode).unwrap();
        serde_json::to_value(schema).unwrap()
    }
}

#[cfg(feature = "validate-jsonschema")]
fn compile_json_schema(ontology: &Ontology, def: &Def) -> jsonschema::Validator {
    let standalone_schema = build_standalone_schema(ontology, def, ProcessorMode::Create).unwrap();

    tracing::debug!(
        "outputted json schema: {}",
        serde_json::to_string_pretty(&standalone_schema).unwrap()
    );

    jsonschema::Validator::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&serde_json::to_value(&standalone_schema).unwrap())
        .unwrap()
}

#[derive(Clone)]
pub struct ValueBuilder<'t, 'on> {
    binding: &'t DefBinding<'on>,
    value: Value,
}

impl ValueBuilder<'_, '_> {
    pub fn to_value(self) -> Value {
        self.value
    }
}

impl<'t, 'on> From<ValueBuilder<'t, 'on>> for Value {
    fn from(b: ValueBuilder<'t, 'on>) -> Self {
        b.value
    }
}

impl<'t, 'on> From<ValueBuilder<'t, 'on>> for Attr {
    fn from(b: ValueBuilder<'t, 'on>) -> Attr {
        b.to_unit_attr()
    }
}

impl ValueBuilder<'_, '_> {
    pub fn relationship(self, name: &str, attr: Attr) -> Self {
        let prop_id = self.binding.find_property(name).expect("unknown property");
        self.merge_attribute(prop_id, attr)
    }

    pub fn to_unit_attr(self) -> Attr {
        Attr::Unit(self.value)
    }

    pub fn to_attr(self, rel_params: impl Into<Value>) -> Attr {
        Attr::Tuple(Box::new(EndoTuple {
            elements: [self.value, rel_params.into()].into_iter().collect(),
        }))
    }

    #[track_caller]
    fn with_json_data(mut self, json: serde_json::Value) -> Self {
        let value = serde_create(self.binding)
            .to_attr_nocheck(json)
            .unwrap()
            .into_unit()
            .unwrap();
        self.value.tag_mut().set_def_id(value.type_def_id());
        match (&mut self.value, value) {
            (Value::Unit(_), value) => {
                self.value = value;
            }
            (Value::Struct(attrs_a, _), Value::Struct(attrs_b, _)) => {
                attrs_a.extend(*attrs_b);
            }
            (a, b) => panic!("Unable to merge {a:?} and {b:?}"),
        }
        self
    }

    fn with_json_id(self, json: serde_json::Value) -> Self {
        let entity = self.binding.def.entity().expect("Not an entity!");
        let id = self
            .binding
            .ontology
            .new_serde_processor(entity.id_operator_addr, ProcessorMode::Create)
            .deserialize(&mut serde_json::Deserializer::from_str(
                &serde_json::to_string(&json).unwrap(),
            ))
            .unwrap();

        self.merge_attribute(entity.id_prop, id)
    }

    pub fn with_open_data(self, json: serde_json::Value) -> Self {
        let value = deserialize_raw(
            ProcessorLevel::new_root(),
            &mut serde_json::Deserializer::from_str(&serde_json::to_string(&json).unwrap()),
        )
        .unwrap();
        self.merge_attribute(PropId::open_data(), value.into())
    }

    fn merge_attribute(mut self, prop_id: PropId, attr: Attr) -> Self {
        match &mut self.value {
            Value::Struct(attrs, _) => {
                attrs.insert(prop_id, attr);
            }
            Value::Unit(def_id) => {
                self.value = Value::new_struct([(prop_id, attr)], *def_id);
            }
            other => {
                panic!("Value data was not a map/unit, but {other:?}.")
            }
        }
        self
    }
}
