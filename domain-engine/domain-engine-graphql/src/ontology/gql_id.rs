use std::fmt::Display;

use ::juniper::FieldResult;
use ulid::Ulid;

use crate::{gql_scalar::GqlScalar, juniper};

use super::OntologyCtx;

#[derive(juniper::GraphQLScalar)]
#[graphql(
    scalar = GqlScalar,
    to_output_with = DefId::to_output,
    from_input_with = DefId::from_input,
    parse_token_with = DefId::parse_token,
)]
pub struct DefId {
    pub domain_id: Ulid,
    pub def_tag: u16,
}

impl Display for DefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.domain_id, self.def_tag)
    }
}

impl DefId {
    pub fn as_runtime_def_id(&self, ctx: &OntologyCtx) -> FieldResult<ontol_runtime::DefId> {
        let domain = ctx
            .domain_by_id(self.domain_id)
            .ok_or("def domain not found")?;
        Ok(ontol_runtime::DefId(
            domain.def_id().domain_index(),
            self.def_tag,
        ))
    }

    fn to_output(v: &Self) -> juniper::Value<GqlScalar> {
        use std::fmt::Write;
        let mut s = smartstring::alias::String::new();
        write!(&mut s, "{v}").unwrap();
        juniper::Value::Scalar(GqlScalar::String(s))
    }

    fn from_input(input: &juniper::InputValue<GqlScalar>) -> Result<Self, String> {
        let juniper::InputValue::Scalar(GqlScalar::String(string)) = input else {
            return Err(DefIdParseError::MustBeAString.into());
        };

        let (domain_id, tag) = string.split_once(":").ok_or(DefIdParseError::BadFormat)?;
        let ulid = Ulid::from_string(domain_id).map_err(|_| DefIdParseError::BadFormat)?;
        let def_tag: u16 = tag.parse().map_err(|_| DefIdParseError::BadFormat)?;

        Ok(DefId {
            domain_id: ulid,
            def_tag,
        })
    }

    fn parse_token(token: juniper::ScalarToken<'_>) -> juniper::ParseScalarResult<GqlScalar> {
        if let juniper::ScalarToken::String(s) = token {
            Ok(GqlScalar::String(s.into()))
        } else {
            Err(juniper::ParseError::ExpectedScalarError(
                "must be stringly typed",
            ))
        }
    }
}

enum DefIdParseError {
    MustBeAString,
    BadFormat,
}

impl From<DefIdParseError> for String {
    fn from(value: DefIdParseError) -> Self {
        match value {
            DefIdParseError::MustBeAString => "must be a string".to_string(),
            DefIdParseError::BadFormat => "badly formatted domain def id".to_string(),
        }
    }
}
