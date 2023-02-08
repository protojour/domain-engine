use ontol_runtime::{
    env::Env,
    serde::SerdeOperatorId,
    value::{Data, Value},
    DefId,
};
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
        let def_id = domain
            .get_def_id(type_name)
            .unwrap_or_else(|| panic!("type name not found: `{type_name}`"));
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

    pub fn deserialize_data(
        &self,
        env: &Env,
        json: serde_json::Value,
    ) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(env, json)?;
        assert_eq!(value.type_def_id, self.def_id);
        Ok(value.data)
    }

    /// Deserialize data, but expect that the resulting type DefId
    /// is not the same as the nominal one for the TypeBinding.
    /// (i.e. it should deserialize to a _variant_ of the type)
    pub fn deserialize_data_variant(
        &self,
        env: &Env,
        json: serde_json::Value,
    ) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(env, json)?;
        assert_ne!(value.type_def_id, self.def_id);
        Ok(value.data)
    }

    pub fn deserialize_value(
        &self,
        env: &Env,
        json: serde_json::Value,
    ) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();
        let (edge, value) = env
            .new_serde_processor(self.serde_operator_id)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))?;

        assert_eq!(edge.type_def_id, DefId::unit());

        Ok(value)
    }

    pub fn serialize_json(&self, env: &Env, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        env.new_serde_processor(self.serde_operator_id)
            .serialize_value(&value, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}

pub fn serialize_json(env: &Env, value: &Value) -> serde_json::Value {
    let serde_operator_id = env.serde_operators_per_def.get(&value.type_def_id).unwrap();
    let mut buf: Vec<u8> = vec![];
    env.new_serde_processor(*serde_operator_id)
        .serialize_value(&value, &mut serde_json::Serializer::new(&mut buf))
        .expect("serialization failed");
    serde_json::from_slice(&buf).unwrap()
}
