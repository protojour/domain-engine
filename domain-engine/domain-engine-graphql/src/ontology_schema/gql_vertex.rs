use ::juniper::{FieldResult, GraphQLObject};
use base64::Engine;
use fnv::FnvHashMap;
use ontol_runtime::{attr::Attr, sequence::Sequence, value::Value, DefId, PropId};

use crate::{cursor_util::serialize_cursor, field_error, juniper};

use super::{gql_def, OntologyCtx};

pub struct Vertex {
    pub def_id: DefId,
    pub attrs: FnvHashMap<PropId, Attr>,
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx)]
impl Vertex {
    /// The data store address of this vertex
    fn address(&self, ctx: &OntologyCtx) -> Option<juniper::ID> {
        match self
            .attrs
            .get(&ctx.ontol_domain_meta().data_store_address_prop_id())?
            .as_unit()?
        {
            Value::OctetSequence(seq, _) => Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&seq.0)
                    .into(),
            ),
            _ => None,
        }
    }

    fn def(&self) -> gql_def::Def {
        gql_def::Def { id: self.def_id }
    }
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx)]
pub struct VertexConnection {
    pub elements: Vec<Vertex>,
    pub page_info: Option<PageInfo>,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

impl VertexConnection {
    pub fn from_sequence(sequence: Sequence<Value>) -> FieldResult<Self> {
        let (elements, sub_seq) = sequence.split();

        let elements = elements
            .into_iter()
            .map(|value| match value {
                Value::Struct(attrs, tag) => Ok(Vertex {
                    def_id: tag.into(),
                    attrs: *attrs,
                }),
                _ => Err(field_error("vertex must be a struct")),
            })
            .collect::<FieldResult<_>>()?;

        Ok(Self {
            elements,
            page_info: sub_seq.map(|sub_seq| PageInfo {
                has_next_page: sub_seq.has_next,
                end_cursor: sub_seq.end_cursor.map(|c| serialize_cursor(&c)),
            }),
        })
    }
}
