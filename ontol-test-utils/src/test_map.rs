use ontol_runtime::{
    condition::Condition,
    interface::serde::processor::ProcessorMode,
    ontology::{Extern, ValueCardinality},
    value::Value,
    vm::{
        proc::{Procedure, Yield},
        VmResult, VmState,
    },
    MapDef, MapDefFlags, MapFlags, MapKey,
};
use serde::de::DeserializeSeed;
use unimock::{unimock, Unimock};

use crate::{
    expect_eq,
    serde_helper::{serde_raw, SerdeHelper},
    type_binding::TypeBinding,
    OntolTest,
};

pub trait AsKey: Sized + Clone {
    fn as_key(&self) -> Key;
    fn unit(&self) -> Key {
        self.as_key()
    }
    fn seq(&self) -> Key {
        self.as_key()
    }
}

impl AsKey for &'static str {
    fn as_key(&self) -> Key {
        self.unit()
    }

    fn unit(&self) -> Key {
        Key::Unit(self)
    }

    fn seq(&self) -> Key {
        Key::Seq(self)
    }
}

#[derive(Clone)]
pub enum Key {
    Unit(&'static str),
    Seq(&'static str),
}

impl AsKey for Key {
    fn as_key(&self) -> Key {
        self.clone()
    }
}

impl Key {
    fn typename(&self) -> &'static str {
        match self {
            Self::Unit(t) => t,
            Self::Seq(t) => t,
        }
    }
}

impl OntolTest {
    pub fn mapper(&self) -> TestMapper<'_, '_> {
        TestMapper {
            test: self,
            yielder: Box::new(Unimock::new(())),
            serde_helper_factory: Box::new(serde_raw),
        }
    }
}

#[unimock(api = YielderMock)]
pub trait Yielder {
    fn yield_match(&self, value_cardinality: ValueCardinality, condition: Condition) -> Value;
    fn yield_call_extern_http_json(&self, url: &str, body: serde_json::Value) -> serde_json::Value;
}

pub struct TestMapper<'on, 'p> {
    test: &'on OntolTest,
    yielder: Box<dyn Yielder>,
    serde_helper_factory: Box<dyn for<'b> Fn(&'b TypeBinding<'on>) -> SerdeHelper<'b, 'on, 'p>>,
}

impl<'on, 'p> TestMapper<'on, 'p> {
    pub fn with_mock_yielder(self, yielder_mock_clause: impl unimock::Clause) -> Self {
        Self {
            yielder: Box::new(Unimock::new(yielder_mock_clause)),
            ..self
        }
    }

    pub fn with_serde_helper(
        self,
        factory: impl for<'b> Fn(&'b TypeBinding<'on>) -> SerdeHelper<'b, 'on, 'p> + 'static,
    ) -> Self {
        Self {
            serde_helper_factory: Box::new(factory),
            ..self
        }
    }

    /// Assert that the serialized output of the mapping of the json-encoded input value matches the expected json document
    #[track_caller]
    pub fn assert_map_eq(
        &self,
        (from, to): (impl AsKey, impl AsKey),
        input: serde_json::Value,
        expected: serde_json::Value,
    ) {
        let from = from.as_key();
        let to = to.as_key();

        let value = self.domain_map((from.clone(), to.clone()), input).unwrap();
        let [output_binding] = self.test.bind([to.typename()]);
        let output_json = match &to {
            Key::Unit(_) => (*self.serde_helper_factory)(&output_binding).as_json(&value),
            Key::Seq(_) => {
                (*self.serde_helper_factory)(&output_binding).dynamic_seq_as_json(&value)
            }
        };

        expect_eq!(actual = output_json, expected = expected);
    }

    #[track_caller]
    pub fn assert_named_forward_map(
        &self,
        name: &str,
        input: serde_json::Value,
        expected: serde_json::Value,
    ) {
        let (package_id, name) = self.test.parse_test_ident(name);
        let key = self
            .test
            .ontology
            .find_named_forward_map_meta(package_id, name)
            .unwrap();

        let input_binding = TypeBinding::from_def_id(key.input.def_id, &self.test.ontology);
        let output_binding = TypeBinding::from_def_id(key.output.def_id, &self.test.ontology);

        let procedure = match self.test.ontology.get_mapper_proc(&key) {
            Some(procedure) => procedure,
            None => panic!("named map not found"),
        };
        let param = (*self.serde_helper_factory)(&input_binding)
            .to_value_nocheck(input)
            .unwrap();
        let value = self.run_vm(procedure, param).unwrap();

        // The resulting value must have the runtime def_id of the requested to_key.
        expect_eq!(actual = value.type_def_id(), expected = key.output.def_id);

        let output_json = if key.output.flags.contains(MapDefFlags::SEQUENCE) {
            (*self.serde_helper_factory)(&output_binding).dynamic_seq_as_json(&value)
        } else {
            (*self.serde_helper_factory)(&output_binding).as_json(&value)
        };

        expect_eq!(actual = output_json, expected = expected);
    }

    /// Use mapping procedure to map the json-encoded input value to an output value.
    #[track_caller]
    pub fn domain_map(
        &self,
        (from, to): (impl AsKey, impl AsKey),
        input: serde_json::Value,
    ) -> VmResult<Value> {
        let from = from.as_key();
        let to = to.as_key();

        let [input_binding, output_binding] = self.test.bind([from.typename(), to.typename()]);
        let param = (*self.serde_helper_factory)(&input_binding)
            .to_value_nocheck(input)
            .unwrap();

        fn get_map_def(key: &Key, binding: &TypeBinding) -> MapDef {
            let mut flags = MapDefFlags::empty();
            if matches!(key, Key::Seq(_)) {
                flags.insert(MapDefFlags::SEQUENCE);
            }
            MapDef {
                def_id: binding.type_info.def_id,
                flags,
            }
        }

        let input_def = get_map_def(&from, &input_binding);
        let output_def = get_map_def(&to, &output_binding);

        let procedure = match self.test.ontology.get_mapper_proc(&MapKey {
            input: input_def,
            output: output_def,
            flags: MapFlags::empty(),
        }) {
            Some(procedure) => procedure,
            None => panic!(
                "No mapping procedure found for ({:?}, {:?})",
                input_binding.type_info.def_id, output_binding.type_info.def_id
            ),
        };

        let value = self.run_vm(procedure, param)?;

        // The resulting value must have the runtime def_id of the requested to_key.
        expect_eq!(actual = value.type_def_id(), expected = output_def.def_id);

        Ok(value)
    }

    fn run_vm(&self, procedure: Procedure, mut param: Value) -> VmResult<Value> {
        let mut vm = self.test.ontology.new_vm(procedure);
        loop {
            match vm.run([param])? {
                VmState::Complete(value) => return Ok(value),
                VmState::Yield(Yield::Match(_var, cardinality, condition)) => {
                    param = self.yielder.yield_match(cardinality, condition);
                }
                VmState::Yield(Yield::CallExtern(extern_def_id, extern_param, output_def_id)) => {
                    let ontology = &self.test.ontology;
                    let input_type_info = ontology.get_type_info(extern_param.type_def_id());
                    let output_type_info = ontology.get_type_info(output_def_id);

                    let param_json: serde_json::Value = {
                        let mut buf: Vec<u8> = vec![];
                        ontology
                            .new_serde_processor(
                                input_type_info.operator_addr.unwrap(),
                                ProcessorMode::Read,
                            )
                            .serialize_value(
                                &extern_param,
                                None,
                                &mut serde_json::Serializer::new(&mut buf),
                            )
                            .unwrap();
                        serde_json::from_slice(&buf).unwrap()
                    };

                    match ontology.get_extern(extern_def_id) {
                        Some(Extern::HttpJson { url }) => {
                            let output_json = self
                                .yielder
                                .yield_call_extern_http_json(&self.test.ontology[*url], param_json);

                            let attr = ontology
                                .new_serde_processor(
                                    output_type_info.operator_addr.unwrap(),
                                    ProcessorMode::Read,
                                )
                                .deserialize(output_json)
                                .unwrap();

                            param = attr.val;
                        }
                        _ => panic!("unhandled"),
                    }
                }
            }
        }
    }
}
