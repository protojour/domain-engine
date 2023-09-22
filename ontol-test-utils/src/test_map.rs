use ontol_runtime::{value::Value, MapKey};

use crate::{
    expect_eq,
    serde_utils::{inspect_de, inspect_ser},
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
    /// Use mapping procedure to map the json-encoded input value to an output value.
    #[track_caller]
    pub fn domain_map(
        &self,
        (from, to): (impl AsKey, impl AsKey),
        input: serde_json::Value,
    ) -> Value {
        let from = from.as_key();
        let to = to.as_key();

        let [input_binding, output_binding] = self.bind([from.typename(), to.typename()]);
        let value = inspect_de(&input_binding).value(input).unwrap();

        fn get_map_key(key: &Key, binding: &TypeBinding) -> MapKey {
            let seq = matches!(key, Key::Seq(_));
            MapKey {
                def_id: binding.type_info.def_id,
                seq,
            }
        }

        let from_key = get_map_key(&from, &input_binding);
        let to_key = get_map_key(&to, &output_binding);

        let procedure = match self.ontology.get_mapper_proc(from_key, to_key) {
            Some(procedure) => procedure,
            None => panic!(
                "No mapping procedure found for ({:?}, {:?})",
                input_binding.type_info.def_id, output_binding.type_info.def_id
            ),
        };

        let value = self.ontology.new_vm().eval(procedure, [value]);

        // The resulting value must have the runtime def_id of the requested to_key.
        expect_eq!(actual = value.type_def_id, expected = to_key.def_id);

        value
    }

    /// Assert that the serialized output of the mapping of the json-encoded input value matches the expected json document
    #[track_caller]
    pub fn assert_domain_map(
        &self,
        (from, to): (impl AsKey, impl AsKey),
        input: serde_json::Value,
        expected: serde_json::Value,
    ) {
        let from = from.as_key();
        let to = to.as_key();

        let value = self.domain_map((from.clone(), to.clone()), input);
        let [output_binding] = self.bind([to.typename()]);
        let output_json = match &to {
            Key::Unit(_) => inspect_ser(&output_binding).json(&value),
            Key::Seq(_) => inspect_ser(&output_binding).dynamic_sequence_json(&value),
        };

        expect_eq!(actual = output_json, expected = expected);
    }
}
