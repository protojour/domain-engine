use std::{fmt::Display, str::FromStr};

use ::juniper::FieldResult;
use compact_str::CompactString;
use ontol_runtime::DomainId;

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
    pub(crate) domain_id: DomainId,
    pub(crate) def_tag: u16,
    pub(crate) persistent: bool,
}

impl Display for DefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}:{}",
            if self.persistent { "" } else { "~" },
            self.domain_id,
            self.def_tag,
        )
    }
}

impl DefId {
    pub fn new(def_id: ontol_runtime::DefId, ctx: &OntologyCtx) -> Self {
        let domain = ctx.domain_by_index(def_id.domain_index()).unwrap();

        Self {
            domain_id: domain.domain_id().id,
            def_tag: def_id.tag(),
            persistent: def_id.is_persistent(),
        }
    }

    pub fn as_runtime_def_id(&self, ctx: &OntologyCtx) -> FieldResult<ontol_runtime::DefId> {
        let domain = ctx
            .domain_by_id(self.domain_id)
            .ok_or("def domain not found")?;
        Ok(ontol_runtime::DefId::new_persistent(
            domain.def_id().domain_index(),
            self.def_tag,
        ))
    }

    fn to_output(v: &Self) -> juniper::Value<GqlScalar> {
        use std::fmt::Write;
        let mut s = CompactString::default();
        write!(&mut s, "{v}").unwrap();
        juniper::Value::Scalar(GqlScalar::String(s))
    }

    fn from_input(input: &juniper::InputValue<GqlScalar>) -> Result<Self, String> {
        let juniper::InputValue::Scalar(GqlScalar::String(string)) = input else {
            return Err(DefIdParseError::MustBeAString.into());
        };

        let mut string = string.as_str();

        let mut persistent = true;
        if let Some(p) = string.strip_prefix("~") {
            string = p;
            persistent = false;
        }

        let (domain_id, tag) = string.split_once(":").ok_or(DefIdParseError::BadFormat)?;
        let domain_id = DomainId::from_str(domain_id).map_err(|_| DefIdParseError::BadFormat)?;
        let def_tag: u16 = tag.parse().map_err(|_| DefIdParseError::BadFormat)?;

        Ok(DefId {
            domain_id,
            def_tag,
            persistent,
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
