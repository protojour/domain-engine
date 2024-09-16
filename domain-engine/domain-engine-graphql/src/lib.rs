#![forbid(unsafe_code)]

use std::fmt::Display;

use ::juniper::{graphql_value, FieldError};

pub mod cursor_util;
pub mod domain;
pub mod gql_scalar;
pub mod ontology;

pub mod juniper {
    pub use ::juniper::*;
}

fn field_error<S>(msg: impl Display) -> FieldError<S> {
    FieldError::new(msg, graphql_value!(None))
}
