use std::fmt::Display;

use base64::Engine;
use ontol_runtime::{
    attr::{Attr, AttrRef},
    interface::serde::processor::ProcessorMode,
    ontology::domain::DefRepr,
    value::{FormatValueAsText, Value},
    PropId,
};

use crate::{field_error, gql_scalar::GqlScalar};

use super::OntologyCtx;

#[derive(Clone, Copy)]
pub struct ValueScalarCfg {
    pub with_address: bool,
    pub with_def_id: bool,
}

#[derive(juniper::GraphQLScalar)]
#[graphql(
    scalar = GqlScalar,
    to_output_with = OntolValue::to_output,
    from_input_with = OntolValue::from_input,
    parse_token_with = OntolValue::parse_token,
)]
pub struct OntolValue(pub juniper::Value<GqlScalar>);

impl OntolValue {
    fn to_output(v: &Self) -> juniper::Value<GqlScalar> {
        v.0.clone()
    }

    fn from_input(input: &juniper::InputValue<GqlScalar>) -> Result<Self, String> {
        match input {
            juniper::InputValue::Null => Ok(Self(juniper::Value::null())),
            juniper::InputValue::Scalar(scalar) => Ok(Self(juniper::Value::Scalar(scalar.clone()))),
            juniper::InputValue::Enum(_) => Err("enum input not suported".to_string()),
            juniper::InputValue::Variable(_) => Err("variable input not supported".to_string()),
            juniper::InputValue::List(list) => Ok(Self(juniper::Value::List(
                list.iter()
                    .map(|spanning| Self::from_input(&spanning.item).map(|value| value.0))
                    .collect::<Result<_, _>>()?,
            ))),
            juniper::InputValue::Object(attributes) => {
                let mut gobj = juniper::Object::with_capacity(attributes.len());
                for (key, value) in attributes {
                    let value = Self::from_input(&value.item)?;
                    gobj.add_field(&key.item, value.0);
                }

                Ok(Self(juniper::Value::object(gobj)))
            }
        }
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

const DEF_ID: &str = "defId";
const PROP_ID: &str = "propId";
const ATTR: &str = "attr";
const TYPE: &str = "type";
const ADDRESS: &str = "address";
const VALUE: &str = "value";

fn put_string(gobj: &mut juniper::Object<GqlScalar>, key: &str, value: impl Display) {
    gobj.add_field(
        key,
        juniper::Value::Scalar(GqlScalar::String(format!("{value}").into())),
    );
}

pub fn write_ontol_scalar(
    gobj: &mut juniper::Object<GqlScalar>,
    value: Value,
    cfg: ValueScalarCfg,
    ctx: &OntologyCtx,
) -> juniper::FieldResult<()> {
    let def_id = value.type_def_id();
    if cfg.with_def_id {
        put_string(gobj, DEF_ID, format!("{:?}", def_id));
    }

    match value {
        Value::Unit(_) => put_string(gobj, TYPE, "unit"),
        Value::Void(_) => put_string(gobj, TYPE, "void"),
        Value::I64(i, _) => {
            put_string(gobj, TYPE, "i64");
            put_string(gobj, VALUE, i);
        }
        Value::F64(f, _) => {
            put_string(gobj, TYPE, "f64");
            put_string(gobj, VALUE, f);
        }
        Value::Serial(s, _) => {
            put_string(gobj, TYPE, "serial");
            put_string(gobj, VALUE, s.0);
        }
        Value::Rational(r, _) => {
            put_string(gobj, TYPE, "rational");
            put_string(gobj, VALUE, r);
        }
        Value::Text(s, _) => {
            put_string(gobj, TYPE, "text");
            put_string(gobj, VALUE, s);
        }
        Value::OctetSequence(s, _) => {
            put_string(gobj, TYPE, "octets_base64");
            put_string(
                gobj,
                VALUE,
                base64::engine::general_purpose::STANDARD.encode(&s.0),
            );
        }
        value @ Value::ChronoDateTime(..) => {
            put_string(gobj, TYPE, "datetime");
            put_string(
                gobj,
                VALUE,
                &FormatValueAsText {
                    value: &value,
                    type_def_id: value.type_def_id(),
                    ontology: ctx,
                },
            );
        }
        value @ Value::ChronoDate(..) => {
            put_string(gobj, TYPE, "date");
            put_string(
                gobj,
                VALUE,
                &FormatValueAsText {
                    value: &value,
                    type_def_id: value.type_def_id(),
                    ontology: ctx,
                },
            );
        }
        value @ Value::ChronoTime(..) => {
            put_string(gobj, TYPE, "time");
            put_string(
                gobj,
                VALUE,
                &FormatValueAsText {
                    value: &value,
                    type_def_id: value.type_def_id(),
                    ontology: ctx,
                },
            );
        }
        Value::Struct(attrs, tag) => {
            let def = ctx.def(def_id);
            match (def.repr(), def.operator_addr) {
                (Some(DefRepr::FmtStruct(_)), Some(addr)) => {
                    let processor = ctx.new_serde_processor(addr, ProcessorMode::Read);
                    let value = Value::Struct(attrs, tag);
                    let mut json_buf: Vec<u8> = vec![];
                    processor
                        .serialize_attr(
                            AttrRef::Unit(&value),
                            &mut serde_json::Serializer::new(&mut json_buf),
                        )
                        .unwrap();
                    let text_value = match serde_json::from_slice(&json_buf).map_err(field_error)? {
                        serde_json::Value::String(string) => string,
                        _ => return Err(field_error("format error")),
                    };

                    put_string(gobj, TYPE, "text");
                    put_string(gobj, VALUE, text_value);
                }
                _ => {
                    put_string(gobj, TYPE, "struct");

                    if cfg.with_address {
                        if let Some(Value::OctetSequence(seq, _)) = attrs
                            .get(&PropId::data_store_address())
                            .and_then(|attr| attr.as_unit())
                        {
                            put_string(
                                gobj,
                                ADDRESS,
                                base64::engine::general_purpose::STANDARD.encode(&seq.0),
                            );
                        }
                    }

                    let mut sorted: Vec<_> = attrs
                        .into_iter()
                        .filter(|(prop_id, _)| prop_id.0.domain_index().index() != 0)
                        .collect();
                    sorted.sort_by_key(|(prop_id, _)| *prop_id);

                    let mut gattrs = Vec::with_capacity(sorted.len());

                    for (prop_id, attr) in sorted {
                        gattrs.push(ontol_attr_to_scalar(prop_id, attr, cfg, ctx)?);
                    }

                    gobj.add_field("attrs", juniper::Value::list(gattrs));
                }
            }
        }
        Value::Dict(_, _) => todo!(),
        Value::Sequence(_, _) => todo!(),
        Value::DeleteRelationship(..) | Value::Filter(..) => {
            put_string(gobj, TYPE, "error");
        }
    }

    Ok(())
}

pub fn ontol_attr_to_scalar(
    prop_id: PropId,
    attr: Attr,
    cfg: ValueScalarCfg,
    ctx: &OntologyCtx,
) -> juniper::FieldResult<juniper::Value<GqlScalar>> {
    let mut gobj = juniper::Object::with_capacity(2);

    put_string(&mut gobj, PROP_ID, prop_id);

    match attr {
        Attr::Unit(value) => {
            put_string(&mut gobj, ATTR, "unit");
            write_ontol_scalar(&mut gobj, value, cfg, ctx)?;
        }
        Attr::Tuple(_) => {
            put_string(&mut gobj, ATTR, "tuple");
        }
        Attr::Matrix(matrix) => {
            put_string(&mut gobj, ATTR, "matrix");

            let mut gql_columns = Vec::with_capacity(matrix.columns.len());
            for column in matrix.columns {
                let mut gql_column = Vec::with_capacity(column.elements().len());

                let (elements, _) = column.split();
                for value in elements {
                    let mut gobj = juniper::Object::with_capacity(3);
                    write_ontol_scalar(&mut gobj, value, cfg, ctx)?;
                    gql_column.push(juniper::Value::object(gobj));
                }

                gql_columns.push(juniper::Value::list(gql_column));
            }

            gobj.add_field("columns", juniper::Value::list(gql_columns));
        }
    }

    Ok(juniper::Value::Object(gobj))
}
