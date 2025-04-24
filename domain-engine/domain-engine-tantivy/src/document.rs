use std::{collections::BTreeMap, sync::Arc};

use domain_engine_core::{VertexAddr, data_store::DataStoreAPI};
use ontol_runtime::{
    OntolDefTag, OntolDefTagExt,
    attr::Attr,
    ontology::{Ontology, domain::DataRelationshipKind},
    tuple::EndoTuple,
    value::Value,
};
use tantivy::{
    DateTime, TantivyDocument,
    schema::{Facet, OwnedValue},
};
use tracing::{debug, warn};

use crate::schema::SchemaWithMeta;

pub struct IndexingContext {
    pub ontology: Arc<Ontology>,
    pub schema: SchemaWithMeta,
    pub indexing_datastore: Arc<dyn DataStoreAPI + Send + Sync>,
}

pub struct Doc {
    pub vertex_addr: VertexAddr,
    pub create_time: chrono::DateTime<chrono::Utc>,
    pub update_time: chrono::DateTime<chrono::Utc>,
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
    /// no create time
    NoCreateTime,
    /// no update time
    NoUpdateTime,
    /// update-time out of range
    UpdateTimeOutOfRange,
}

impl std::error::Error for DocError {}

impl IndexingContext {
    pub fn make_vertex_doc(&self, vertex: Value) -> Result<Doc, DocError> {
        let def_id = vertex.type_def_id();
        let domain = self.ontology.domain_by_index(def_id.0).unwrap();

        if !domain.domain_id().stable {
            return Err(DocError::UnstableDomain);
        }

        let Value::Struct(attrs, _tag) = &vertex else {
            return Err(DocError::InvalidVertex);
        };

        let mut doc = TantivyDocument::new();

        let vertex_addr: VertexAddr = {
            let Some(Attr::Unit(Value::OctetSequence(vertex_addr, _))) =
                attrs.get(&OntolDefTag::RelationDataStoreAddress.prop_id_0())
            else {
                return Err(DocError::NoAddress);
            };
            vertex_addr.0.iter().copied().collect()
        };

        doc.add_field_value(
            self.schema.vertex_addr,
            OwnedValue::Bytes(vertex_addr.iter().copied().collect()),
        );

        let create_time = {
            let Some(Attr::Unit(Value::ChronoDateTime(create_time, _))) =
                attrs.get(&OntolDefTag::CreateTime.prop_id_0())
            else {
                return Err(DocError::NoCreateTime);
            };
            *create_time
        };

        let update_time = {
            let Some(Attr::Unit(Value::ChronoDateTime(update_time, _))) =
                attrs.get(&OntolDefTag::UpdateTime.prop_id_0())
            else {
                return Err(DocError::NoUpdateTime);
            };
            *update_time
        };

        if let Some(nanoseconds) = update_time.timestamp_nanos_opt() {
            doc.add_date(
                self.schema.update_time,
                tantivy::DateTime::from_timestamp_nanos(nanoseconds),
            );
        } else {
            return Err(DocError::UpdateTimeOutOfRange);
        }

        doc.add_field_value(
            self.schema.facet,
            OwnedValue::Facet(
                Facet::from_text(&format!("/def/{}:{}", domain.domain_id().id, def_id.1)).unwrap(),
            ),
        );
        doc.add_field_value(
            self.schema.facet,
            OwnedValue::Facet(
                Facet::from_text(&format!("/domain/{}", domain.domain_id().id)).unwrap(),
            ),
        );

        if true {
            let mut concat = String::new();
            self.concat_vertex(&vertex, &mut concat);

            doc.add_field_value(self.schema.text, OwnedValue::Str(concat));
        }

        if let Some(tantivy_value) = self.value_to_tantivy(vertex) {
            debug!("data: {tantivy_value:#?}");
            doc.add_field_value(self.schema.data, tantivy_value);
        }

        Ok(Doc {
            vertex_addr,
            create_time,
            update_time,
            doc,
        })
    }

    fn value_to_tantivy(&self, value: Value) -> Option<OwnedValue> {
        match value {
            Value::Unit(_) | Value::Void(_) => None,
            Value::I64(int, _) => Some(OwnedValue::I64(int)),
            Value::F64(float, _) => Some(OwnedValue::F64(float)),
            Value::Serial(serial, _) => Some(OwnedValue::U64(serial.0)),
            Value::Rational(_, _) => None,
            Value::Text(text, _) => Some(OwnedValue::Str(text.into())),
            Value::OctetSequence(_seq, _) => {
                // FIXME: Must convert this into textual representation
                None
            }
            Value::ChronoDateTime(dt, _) => dt
                .timestamp_nanos_opt()
                // BUG: nanosecond precision + i64 limits historical dates to ~584 years before 1970.
                // so likely a different (text based) datetime solution is needed
                .map(|nanos| OwnedValue::Date(DateTime::from_timestamp_nanos(nanos))),
            Value::ChronoDate(_, _) => {
                debug!("TODO: date");
                None
            }
            Value::ChronoTime(_, _) => {
                debug!("TODO: time");
                None
            }
            Value::Struct(attrs, _) => {
                let mut tantivy_map: BTreeMap<String, OwnedValue> = BTreeMap::new();

                for (prop_id, attr) in attrs.into_iter() {
                    let Some(stable_index) = self.indexing_datastore.stable_property_index(prop_id)
                    else {
                        debug!("stable index for {prop_id:?} was None");
                        continue;
                    };

                    let Some(tantivy_value) = self.attr_to_tantivy(attr) else {
                        continue;
                    };

                    tantivy_map.insert(stable_index.to_string(), tantivy_value);
                }

                Some(OwnedValue::Object(tantivy_map))
            }
            Value::CrdtStruct(..) => None,
            Value::Dict(_dict, _) => None,
            Value::Sequence(_, _) => None,
            Value::DeleteRelationship(_) => None,
            Value::Filter(_, _) => None,
        }
    }

    fn attr_to_tantivy(&self, attr: Attr) -> Option<OwnedValue> {
        match attr {
            Attr::Unit(value) => self.value_to_tantivy(value),
            Attr::Tuple(tuple) => Some(OwnedValue::Array(
                tuple
                    .elements
                    .into_iter()
                    .filter_map(|value| self.value_to_tantivy(value))
                    .collect(),
            )),
            Attr::Matrix(matrix) => Some(OwnedValue::Array(
                matrix
                    .into_rows()
                    .map(|row| {
                        OwnedValue::Array(
                            row.elements
                                .into_iter()
                                .filter_map(|value| self.value_to_tantivy(value))
                                .collect(),
                        )
                    })
                    .collect(),
            )),
        }
    }

    fn concat_vertex(&self, value: &Value, concat: &mut String) {
        use std::fmt::Write;

        match value {
            Value::Unit(_) => {}
            Value::Void(_) => {}
            Value::I64(int, _) => {
                prepend_ws(concat);
                write!(concat, "{int}").unwrap();
            }
            Value::F64(_, _) => {}
            Value::Serial(serial, _) => {
                prepend_ws(concat);
                write!(concat, "{}", serial.0).unwrap();
            }
            Value::Rational(_, _) => {}
            Value::Text(text, _) => {
                prepend_ws(concat);
                concat.push_str(text);
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
                        DataRelationshipKind::Id | DataRelationshipKind::Tree(_) => match attr {
                            Attr::Unit(value) => {
                                self.concat_vertex(value, concat);
                            }
                            Attr::Tuple(tup) => {
                                for element in &tup.elements {
                                    self.concat_vertex(element, concat);
                                }
                            }
                            Attr::Matrix(matrix) => {
                                let mut rows = matrix.rows();
                                let mut tup = EndoTuple::default();

                                while rows.iter_next(&mut tup) {
                                    for element in &tup.elements {
                                        self.concat_vertex(element, concat);
                                    }
                                }
                            }
                        },
                        DataRelationshipKind::Edge(_) => {}
                    }
                }
            }
            Value::CrdtStruct(..) => {}
            Value::Dict(dict, _) => {
                for (key, item) in dict.as_ref() {
                    prepend_ws(concat);
                    write!(concat, "{key}").unwrap();
                    self.concat_vertex(item, concat);
                }
                warn!("TODO: concatenate dict");
            }
            Value::Sequence(seq, _) => {
                for item in seq.elements() {
                    self.concat_vertex(item, concat);
                }
            }
            Value::DeleteRelationship(_) => {}
            Value::Filter(_, _) => {}
        }
    }
}

fn prepend_ws(string: &mut String) {
    if !string.is_empty() {
        string.push(' ');
    }
}
