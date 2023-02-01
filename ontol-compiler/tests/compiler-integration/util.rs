use ontol_runtime::{env::Env, serde::SerdeOperatorId, value::Value, DefId};
use serde::de::DeserializeSeed;
use tracing::debug;

use crate::TEST_PKG;

pub struct TypeBinding {
    pub def_id: DefId,
    serde_operator_id: SerdeOperatorId,
}

impl TypeBinding {
    pub fn new(env: &Env, type_name: &str) -> Self {
        let domain = env.get_domain(&TEST_PKG).unwrap();
        let def_id = domain.get_def_id(type_name).expect("typename not found");
        let serde_operator_id = domain.get_serde_operator_id(type_name).unwrap();
        let binding = Self {
            def_id,
            serde_operator_id,
        };
        debug!(
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
        let (value, serde_def_id) = env
            .new_serde_processor(self.serde_operator_id)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))?;

        assert_eq!(serde_def_id, self.def_id);

        Ok(value)
    }

    /// Special case deserialize where the assertion is that the result DefId
    /// represents the _variant_ instead of the parent type
    pub fn deserialize_variant(
        &self,
        env: &Env,
        json: serde_json::Value,
    ) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();
        let (value, serde_def_id) = env
            .new_serde_processor(self.serde_operator_id)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))?;

        assert_ne!(serde_def_id, self.def_id);

        Ok(value)
    }

    pub fn serialize_json(&self, env: &Env, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        env.new_serde_processor(self.serde_operator_id)
            .serialize_value(value, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}
