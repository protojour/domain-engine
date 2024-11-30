use serde::de::Visitor;

use crate::{attr::Attr, interface::serde::processor::RecursionLimitError};

use super::{operator::SerdeOperatorAddr, processor::SerdeProcessor};

pub struct IdSingletonStructVisitor<'on, 'p> {
    pub processor: SerdeProcessor<'on, 'p>,
    pub property_name: &'on str,
    pub inner_addr: SerdeOperatorAddr,
}

impl<'de> Visitor<'de> for IdSingletonStructVisitor<'_, '_> {
    type Value = Attr;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "id singleton map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let Some(property) = map.next_key::<String>()? else {
            return Err(<A::Error as serde::de::Error>::custom(
                "Expected single property",
            ));
        };

        if property != self.property_name {
            return Err(<A::Error as serde::de::Error>::custom(format!(
                "Expected `{}` property",
                self.property_name
            )));
        }

        let attribute = map.next_value_seed(
            self.processor
                .new_child(self.inner_addr)
                .map_err(RecursionLimitError::to_de_error)?,
        )?;

        Ok(attribute)
    }
}
