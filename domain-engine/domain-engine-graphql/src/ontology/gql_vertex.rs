use ::juniper::{FieldResult, GraphQLEnum, GraphQLInputObject, GraphQLObject};
use ontol_runtime::{sequence::Sequence, value::Value};

use crate::{cursor_util::serialize_cursor, gql_scalar::GqlScalar, juniper};

use super::{
    OntologyCtx, gql_id,
    gql_value::{self, OntolValue, ValueScalarCfg, write_ontol_scalar},
};

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
pub struct VertexConnection {
    pub elements: Vec<gql_value::OntolValue>,
    pub page_info: Option<PageInfo>,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
pub struct VertexSearchConnection {
    pub results: Vec<VertexSearchResult>,
    pub facets: VertexSearchFacets,
    pub page_info: Option<PageInfo>,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
pub struct VertexSearchFacets {
    pub domains: Vec<VertexSearchDomainFacetCount>,
    pub defs: Vec<VertexSearchDefFacetCount>,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
pub struct VertexSearchDomainFacetCount {
    pub domain_id: juniper::ID,
    pub count: i32,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
pub struct VertexSearchDefFacetCount {
    pub def_id: gql_id::DefId,
    pub count: i32,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
pub struct VertexSearchResult {
    pub vertex: gql_value::OntolValue,
    pub score: f64,
}

#[derive(GraphQLObject)]
#[graphql(context = OntologyCtx)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

impl VertexConnection {
    pub fn from_sequence(
        sequence: Sequence<Value>,
        cfg: ValueScalarCfg,
        ctx: &OntologyCtx,
    ) -> FieldResult<Self> {
        let (elements, sub_seq) = sequence.split();

        let elements = elements
            .into_iter()
            .map(|value| {
                let mut gobj = juniper::Object::with_capacity(3);
                write_ontol_scalar(&mut gobj, value, cfg, ctx)
                    .map(|()| OntolValue(juniper::Value::Object(gobj)))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            elements,
            page_info: sub_seq.map(|sub_seq| PageInfo {
                has_next_page: sub_seq.has_next,
                end_cursor: sub_seq.end_cursor.map(|c| serialize_cursor(&c)),
            }),
        })
    }
}

#[derive(GraphQLInputObject)]
pub struct VertexOrder {
    pub field_paths: Vec<Vec<String>>,
    pub direction: Option<VertexOrderDirection>,
}

#[derive(GraphQLEnum)]
pub enum VertexOrderDirection {
    Ascending,
    Descending,
}
