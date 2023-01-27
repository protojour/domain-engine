use ontol_lang::{
    binding::DomainBinding,
    compiler::Compiler,
    serde::{SerdeOperatorOld, SerializeValue},
    Value,
};
use serde::de::DeserializeSeed;

use crate::TEST_PKG;

pub struct TypeBinding<'m> {
    domain_binding: DomainBinding<'m>,
    type_name: String,
}

impl<'m> TypeBinding<'m> {
    pub fn new(compiler: &mut Compiler<'m>, type_name: &str) -> Self {
        let binding = Self {
            domain_binding: compiler.bindings_builder().new_binding(TEST_PKG),
            type_name: type_name.into(),
        };
        println!(
            "deserializing `{type_name}` with operator {:?}",
            binding.operator(compiler)
        );
        binding
    }

    fn operator<'e>(&self, compiler: &'e Compiler<'m>) -> SerdeOperatorOld<'e, 'm> {
        self.domain_binding
            .get_serde_operator_old(compiler, &self.type_name)
            .expect("No serde operator available")
    }

    pub fn deserialize(
        &self,
        compiler: &Compiler<'m>,
        json: serde_json::Value,
    ) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();
        self.operator(compiler)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))
    }

    pub fn serialize_json(&self, compiler: &Compiler<'m>, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        self.operator(compiler)
            .serialize_value(value, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}
