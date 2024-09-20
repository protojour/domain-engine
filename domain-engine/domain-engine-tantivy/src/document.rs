use std::sync::Arc;

use domain_engine_core::VertexAddr;
use ontol_runtime::{
    attr::Attr,
    ontology::{domain::DataRelationshipKind, Ontology},
    value::Value,
    OntolDefTag,
};
use tantivy::{
    schema::{Facet, OwnedValue},
    TantivyDocument,
};
use tracing::debug;

use crate::schema::SchemaWithMeta;

pub struct IndexingContext {
    pub ontology: Arc<Ontology>,
    pub schema: SchemaWithMeta,
}

pub struct Doc {
    pub vertex_addr: VertexAddr,
    pub doc: TantivyDocument,
}

#[derive(displaydoc::Display, Debug)]
pub enum DocError {
    /// unstable domain
    UnstableDomain,
    /// invalid vertex
    InvalidVertex,
    /// no address
    NoAddress,
    /// no update time
    NoUpdateTime,
    /// update-time out of range
    UpdateTimeOutOfRange,
}

impl std::error::Error for DocError {}

impl IndexingContext {
    pub fn make_vertex_doc(&self, vertex: &Value) -> Result<Doc, DocError> {
        let def_id = vertex.type_def_id();
        let domain = self.ontology.domain_by_index(def_id.0).unwrap();

        if !domain.domain_id().stable {
            return Err(DocError::UnstableDomain);
        }

        let Value::Struct(attrs, _tag) = vertex else {
            return Err(DocError::InvalidVertex);
        };

        let mut doc = TantivyDocument::new();

        let Some(Attr::Unit(Value::OctetSequence(vertex_addr, _))) =
            attrs.get(&OntolDefTag::RelationDataStoreAddress.prop_id_0())
        else {
            return Err(DocError::NoAddress);
        };
        doc.add_field_value(
            self.schema.vertex_addr,
            OwnedValue::Bytes(vertex_addr.0.iter().copied().collect()),
        );

        if let Some(Attr::Unit(Value::ChronoDateTime(timestamp, _))) =
            attrs.get(&OntolDefTag::UpdateTime.prop_id_0())
        {
            if let Some(nanoseconds) = timestamp.timestamp_nanos_opt() {
                doc.add_date(
                    self.schema.update_time,
                    tantivy::DateTime::from_timestamp_nanos(nanoseconds),
                );
            } else {
                return Err(DocError::UpdateTimeOutOfRange);
            }
        } else {
            return Err(DocError::NoUpdateTime);
        }

        doc.add_field_value(
            self.schema.domain_def_id,
            OwnedValue::Facet(Facet::from_path([
                domain.domain_id().ulid.to_string(),
                def_id.1.to_string(),
            ])),
        );

        let mut data = VertexData {
            text_concat: String::new(),
        };

        self.process_vertex(vertex, &mut data);

        doc.add_field_value(self.schema.text, OwnedValue::Str(data.text_concat));

        Ok(Doc {
            doc,
            vertex_addr: vertex_addr.0.iter().copied().collect(),
        })
    }

    fn process_vertex(&self, value: &Value, data: &mut VertexData) {
        use std::fmt::Write;

        match value {
            Value::Unit(_) => {}
            Value::Void(_) => {}
            Value::I64(int, _) => {
                prepend_ws(&mut data.text_concat);
                write!(&mut data.text_concat, "{int}").unwrap();
            }
            Value::F64(_, _) => {}
            Value::Serial(serial, _) => {
                prepend_ws(&mut data.text_concat);
                write!(&mut data.text_concat, "{}", serial.0).unwrap();
            }
            Value::Rational(_, _) => {}
            Value::Text(text, _) => {
                prepend_ws(&mut data.text_concat);
                data.text_concat.push_str(text);
            }
            Value::OctetSequence(_, _) => {}
            Value::ChronoDateTime(_, _) => {}
            Value::ChronoDate(_, _) => {}
            Value::ChronoTime(_, _) => {}
            Value::Struct(attrs, _) => {
                let def = self.ontology.def(value.type_def_id());
                for (prop_id, attr) in attrs.iter() {
                    let Some(rel_info) = def.data_relationships.get(prop_id) else {
                        continue;
                    };

                    match &rel_info.kind {
                        DataRelationshipKind::Id | DataRelationshipKind::Tree => match attr {
                            Attr::Unit(value) => {
                                self.process_vertex(value, data);
                            }
                            Attr::Tuple(_) => {
                                debug!("TODO: tuple");
                            }
                            Attr::Matrix(_) => {
                                debug!("TODO: matrix");
                            }
                        },
                        DataRelationshipKind::Edge(_) => {}
                    }
                }
            }
            Value::Dict(_dict, _) => todo!(),
            Value::Sequence(_, _) => todo!(),
            Value::DeleteRelationship(_) => {}
            Value::Filter(_, _) => {}
        }
    }
}

struct VertexData {
    text_concat: String,
}

fn prepend_ws(string: &mut String) {
    if !string.is_empty() {
        string.push(' ');
    }
}
