use ontol_runtime::MapKey;
use ontol_test_utils::{
    expect_eq,
    serde_utils::{inspect_de, inspect_ser},
    type_binding::TypeBinding,
    OntolTest,
};

mod test_map_basic;
mod test_unify_partial;

mod test_map_entity;

pub trait IntoKey: Sized {
    fn into_key(self) -> Key;
    fn unit(self) -> Key {
        self.into_key()
    }
    fn seq(self) -> Key {
        self.into_key()
    }
}

impl IntoKey for &'static str {
    fn into_key(self) -> Key {
        self.unit()
    }

    fn unit(self) -> Key {
        Key::Unit(self)
    }

    fn seq(self) -> Key {
        Key::Seq(self)
    }
}

pub enum Key {
    Unit(&'static str),
    Seq(&'static str),
}

impl IntoKey for Key {
    fn into_key(self) -> Key {
        self
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

pub fn assert_domain_map(
    test: &OntolTest,
    (from, to): (impl IntoKey, impl IntoKey),
    input: serde_json::Value,
    expected: serde_json::Value,
) {
    let from = from.into_key();
    let to = to.into_key();

    let input_binding = TypeBinding::new(test, from.typename());
    let output_binding = TypeBinding::new(test, to.typename());

    let value = inspect_de(&input_binding).value(input).unwrap();

    fn get_map_key(key: &Key, binding: &TypeBinding) -> MapKey {
        let seq = matches!(key, Key::Seq(_));
        MapKey {
            def_id: binding.type_info.def_id,
            seq,
        }
    }

    let procedure = match test.ontology.get_mapper_proc(
        get_map_key(&from, &input_binding),
        get_map_key(&to, &output_binding),
    ) {
        Some(procedure) => procedure,
        None => panic!(
            "No mapping procedure found for ({:?}, {:?})",
            input_binding.type_info.def_id, output_binding.type_info.def_id
        ),
    };

    let mut mapper = test.ontology.new_vm();
    let value = mapper.eval(procedure, [value]);

    let output_json = match &to {
        Key::Unit(_) => inspect_ser(&output_binding).json(&value),
        Key::Seq(_) => inspect_ser(&output_binding).dynamic_sequence_json(&value),
    };

    expect_eq!(actual = output_json, expected = expected);
}
