use crate::{env::Env, PackageId};

pub struct JsonSchema<'e> {
    env: &'e Env,
}

impl<'e> JsonSchema<'e> {
    pub fn build(env: &'e Env, package_id: PackageId) -> Self {
        Self { env }
    }
}
