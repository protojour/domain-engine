use ontol_lang::{
    env::Env,
    serde::{SerdeOperatorId, SerializeValue},
    Value,
};
use serde::de::DeserializeSeed;

use crate::TEST_PKG;

pub struct TypeBinding {
    // domain_binding: DomainBinding<'m>,
    serde_operator_id: SerdeOperatorId,
}

impl TypeBinding {
    pub fn new(env: &Env, type_name: &str) -> Self {
        let serde_operator_id = env
            .get_domain(&TEST_PKG)
            .unwrap()
            .get_serde_operator_id(type_name)
            .unwrap();
        let binding = Self { serde_operator_id };
        println!(
            "deserializing `{type_name}` with processor {:?}",
            env.new_serde_processor(serde_operator_id)
        );
        binding
    }

    pub fn deserialize(
        &self,
        env: &Env,
        json: serde_json::Value,
    ) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();
        env.new_serde_processor(self.serde_operator_id)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))
    }

    pub fn serialize_json(&self, env: &Env, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        env.new_serde_processor(self.serde_operator_id)
            .serialize_value(value, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}
