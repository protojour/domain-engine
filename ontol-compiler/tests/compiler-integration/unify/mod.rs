use ontol_runtime::MapKey;
use ontol_test_utils::{expect_eq, type_binding::TypeBinding, TestEnv};

mod test_map_basic;
mod test_unify_partial;

mod test_map_entity;

trait IntoKey: Sized {
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

impl IntoKey for Key {
    fn into_key(self) -> Key {
        self
    }
}

enum Key {
    Unit(&'static str),
    Seq(&'static str),
}

impl Key {
    fn typename(&self) -> &'static str {
        match self {
            Self::Unit(t) => t,
            Self::Seq(t) => t,
        }
    }
}

fn assert_domain_map(
    test_env: &TestEnv,
    (from, to): (impl IntoKey, impl IntoKey),
    input: serde_json::Value,
    expected: serde_json::Value,
) {
    let from = from.into_key();
    let to = to.into_key();

    let input_binding = TypeBinding::new(test_env, from.typename());
    let output_binding = TypeBinding::new(test_env, to.typename());

    let value = input_binding.de_create().value(input).unwrap();

    fn get_map_key(key: &Key, binding: &TypeBinding) -> MapKey {
        let seq = matches!(key, Key::Seq(_));
        MapKey {
            def_id: binding.type_info.def_id,
            seq,
        }
    }

    let procedure = match test_env.env.get_mapper_proc(
        get_map_key(&from, &input_binding),
        get_map_key(&to, &output_binding),
    ) {
        Some(procedure) => procedure,
        None => panic!(
            "No mapping procedure found for ({:?}, {:?})",
            input_binding.type_info.def_id, output_binding.type_info.def_id
        ),
    };

    let mut mapper = test_env.env.new_vm();
    let value = mapper.eval(procedure, [value]);

    let output_json = match &to {
        Key::Unit(_) => output_binding.ser_create().json(&value),
        Key::Seq(_) => output_binding.ser_create().dynamic_sequence_json(&value),
    };

    expect_eq!(actual = output_json, expected = expected);
}
