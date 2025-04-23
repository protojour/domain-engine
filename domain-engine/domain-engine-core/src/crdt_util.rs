use std::str::FromStr;

use automerge::{
    ActorId, Automerge, AutomergeError, ChangeHash, ObjId, ObjType, ReadDoc, ScalarValue,
    Value as AmValue,
    transaction::{Transactable, Transaction},
};
use fnv::FnvHashMap;
use ontol_runtime::{
    DefId, DefPropTag, DomainId, PropId,
    attr::{Attr, AttrMatrix},
    crdt::CrdtStruct,
    ontology::{
        aspects::DefsAspect,
        domain::{Def, DefRepr},
    },
    sequence::Sequence,
    value::{OctetSequence, Value, ValueTag},
};
use smallvec::smallvec;
use tracing::error;

use crate::{
    DomainResult, domain_error::DomainErrorKind, make_interfacable::MakeInterfacable,
    make_storable::MakeStorable,
};

enum ToCrdtError {
    Automerge(AutomergeError),
    BadInput(&'static str),
}

impl From<AutomergeError> for ToCrdtError {
    fn from(value: AutomergeError) -> Self {
        Self::Automerge(value)
    }
}

enum FromCrdtError {
    Automerge(AutomergeError),
    Datastore(&'static str),
}

impl From<AutomergeError> for FromCrdtError {
    fn from(value: AutomergeError) -> Self {
        Self::Automerge(value)
    }
}

enum AmProperty<'a> {
    MapAttr(ObjId, &'a str),
    ListItem(ObjId, usize),
}

impl<'e> MakeStorable<'e> {
    pub(crate) fn ontol_to_automerge(&self, value: Value) -> DomainResult<Value> {
        let mut am = Automerge::new().with_actor(ActorId::from(self.system.crdt_system_actor()));
        let mut txn = am.transaction();

        let Value::Struct(attrs, tag) = value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        self.write_struct_attrs(
            *attrs,
            self.defs.def(tag.def_id()),
            automerge::ROOT,
            &mut txn,
        )
        .map_err(|err| match err {
            ToCrdtError::Automerge(automerge) => {
                error!("to-automerge error: {automerge:?}");
                DomainErrorKind::Interface("CRDT problem".to_string())
            }
            ToCrdtError::BadInput(msg) => DomainErrorKind::BadInputData(msg.to_string()),
        })?;

        txn.commit();

        Ok(Value::CrdtStruct(CrdtStruct(Box::new(am)), tag))
    }

    fn write_struct_attrs(
        &self,
        mut attrs: FnvHashMap<PropId, Attr>,
        def: &'e Def,
        parent_map: ObjId,
        txn: &mut Transaction,
    ) -> Result<(), ToCrdtError> {
        for (prop_id, _rel_info) in &def.data_relationships {
            let Some(attr) = attrs.remove(prop_id) else {
                continue;
            };

            let prop_name = self.struct_prop_name(*prop_id);

            self.write_attr(
                AmProperty::MapAttr(parent_map.clone(), &prop_name),
                attr,
                txn,
            )?;
        }

        Ok(())
    }

    fn write_attr(
        &self,
        am_prop: AmProperty,
        attr: Attr,
        txn: &mut Transaction,
    ) -> Result<(), ToCrdtError> {
        match attr {
            Attr::Unit(Value::Unit(_)) => Ok(()),
            Attr::Unit(Value::Void(_)) => Ok(()),
            Attr::Unit(Value::I64(i, _)) => Ok(am_prop.write_scalar(ScalarValue::Int(i), txn)?),
            Attr::Unit(Value::F64(f, _)) => Ok(am_prop.write_scalar(ScalarValue::F64(f), txn)?),
            Attr::Unit(Value::Serial(_, _)) => {
                Err(ToCrdtError::BadInput("serial not supported yet"))
            }
            Attr::Unit(Value::Rational(_, _)) => {
                Err(ToCrdtError::BadInput("rational not supported"))
            }
            Attr::Unit(Value::Text(text, _)) => {
                let text_id = am_prop.write_object(ObjType::Text, txn)?;
                Ok(txn.splice_text(text_id, 0, 0, text.as_str())?)
            }
            Attr::Unit(Value::OctetSequence(octets, _)) => {
                am_prop.write_scalar(ScalarValue::Bytes(octets.0.into_iter().collect()), txn)?;
                Ok(())
            }
            Attr::Unit(Value::ChronoDateTime(_, _)) => {
                Err(ToCrdtError::BadInput("date-time not supported yet"))
            }
            Attr::Unit(Value::ChronoDate(_, _)) => {
                Err(ToCrdtError::BadInput("date not supported yet"))
            }
            Attr::Unit(Value::ChronoTime(_, _)) => {
                Err(ToCrdtError::BadInput("time not supported yet"))
            }
            Attr::Unit(Value::Struct(attrs, tag)) => {
                let map_id = am_prop.write_object(ObjType::Map, txn)?;
                let def = self.defs.def(tag.def_id());
                self.write_struct_attrs(*attrs, def, map_id, txn)?;
                Ok(())
            }
            Attr::Unit(Value::CrdtStruct(_, _)) => {
                Err(ToCrdtError::BadInput("cannot store a CRDT within a CRDT"))
            }
            Attr::Unit(Value::Dict(_, _)) => Err(ToCrdtError::BadInput("dict not supported yet")),
            Attr::Unit(Value::Sequence(seq, _)) => {
                let list_id = am_prop.write_object(ObjType::List, txn)?;

                for (index, value) in seq.into_elements().into_iter().enumerate() {
                    self.write_attr(
                        AmProperty::ListItem(list_id.clone(), index),
                        Attr::Unit(value),
                        txn,
                    )?;
                }

                Ok(())
            }
            Attr::Unit(Value::DeleteRelationship(_)) => Ok(()),
            Attr::Unit(Value::Filter(_, _)) => Ok(()),
            Attr::Matrix(matrix) => {
                if matrix.columns.len() != 1 {
                    return Err(ToCrdtError::BadInput(
                        "matrix column width must be 1 for CRDT",
                    ));
                }

                let list_id = am_prop.write_object(ObjType::List, txn)?;

                for (index, row) in matrix.into_rows().enumerate() {
                    let value = row.elements.into_iter().next().unwrap();
                    self.write_attr(
                        AmProperty::ListItem(list_id.clone(), index),
                        Attr::Unit(value),
                        txn,
                    )?;
                }
                Ok(())
            }
            Attr::Tuple(_) => Err(ToCrdtError::BadInput("tuple attr")),
        }
    }

    fn struct_prop_name(&self, prop_id: PropId) -> String {
        let def_id = prop_id.0;

        let domain = self.defs.domain_by_index(def_id.domain_index()).unwrap();
        let domain_id = domain.domain_id().id;
        let def_tag = def_id.1;
        let prop_tag = prop_id.1.0;

        format!("{domain_id}_{def_tag}_{prop_tag}")
    }
}

impl AmProperty<'_> {
    fn write_scalar(self, scalar: ScalarValue, txn: &mut Transaction) -> Result<(), ToCrdtError> {
        match self {
            AmProperty::MapAttr(obj_id, key) => txn.put(obj_id, key, scalar)?,
            AmProperty::ListItem(list_id, index) => {
                txn.insert(list_id, index, scalar)?;
            }
        }

        Ok(())
    }

    fn write_object(self, obj_type: ObjType, txn: &mut Transaction) -> Result<ObjId, ToCrdtError> {
        match self {
            AmProperty::MapAttr(obj_id, key) => Ok(txn.put_object(obj_id, key, obj_type)?),
            AmProperty::ListItem(list_id, index) => {
                Ok(txn.insert_object(list_id, index, obj_type)?)
            }
        }
    }
}

impl MakeInterfacable<'_> {
    pub(crate) fn automerge_to_ontol(&self, am: &Automerge, def_id: DefId) -> DomainResult<Value> {
        let deserializer = AutomergeDeserializer {
            defs: self.defs,
            automerge: am,
            heads: am.get_heads(),
        };

        let def = self.defs.def(def_id);
        let result =
            deserializer.deserialize_value(&automerge::ROOT, AmValue::Object(ObjType::Map), def);
        match result {
            Ok(value) => Ok(value),
            Err(FromCrdtError::Automerge(err)) => {
                Err(DomainErrorKind::DataStore(format!("{err:?}")).into_error())
            }
            Err(FromCrdtError::Datastore(msg)) => {
                Err(DomainErrorKind::DataStore(format!("crdt propble: {msg}")).into_error())
            }
        }
    }
}

pub struct AutomergeDeserializer<'a> {
    defs: &'a DefsAspect,
    automerge: &'a Automerge,
    heads: Vec<ChangeHash>,
}

impl<'a> AutomergeDeserializer<'a> {
    fn deserialize_value(
        &self,
        obj_id: &ObjId,
        value: automerge::Value<'a>,
        def: &Def,
    ) -> Result<Value, FromCrdtError> {
        match self.deserialize_attr(obj_id, value, def)? {
            Attr::Unit(value) => Ok(value),
            Attr::Tuple(_) => Err(FromCrdtError::Datastore("tuple")),
            Attr::Matrix(_) => Err(FromCrdtError::Datastore("matrix")),
        }
    }

    fn deserialize_attr(
        &self,
        obj_id: &ObjId,
        value: automerge::Value<'a>,
        def: &Def,
    ) -> Result<Attr, FromCrdtError> {
        match value {
            AmValue::Object(ObjType::Map | ObjType::Table) => {
                Ok(Attr::Unit(self.deserialize_map(obj_id, def)?))
            }
            AmValue::Object(ObjType::List) => self.deserialize_list(obj_id, def),
            AmValue::Object(ObjType::Text) => Ok(Attr::Unit(self.deserialize_text(obj_id, def)?)),
            AmValue::Scalar(scalar) => {
                let tag: ValueTag = def.id.into();
                match (scalar.as_ref(), def.repr()) {
                    (ScalarValue::Bytes(bytes), Some(DefRepr::Octets)) => Ok(Attr::Unit(
                        Value::OctetSequence(OctetSequence(bytes.iter().copied().collect()), tag),
                    )),
                    (ScalarValue::Str(s), Some(DefRepr::Text)) => Ok(Attr::Unit(Value::Text(
                        FromIterator::from_iter(s.chars()),
                        tag,
                    ))),
                    (ScalarValue::Int(i), Some(DefRepr::I64)) => {
                        Ok(Attr::Unit(Value::I64(*i, tag)))
                    }
                    (ScalarValue::Uint(_), _) => Err(FromCrdtError::Datastore("invalid: uint")),
                    (ScalarValue::F64(f), Some(DefRepr::F64)) => {
                        Ok(Attr::Unit(Value::F64(*f, tag)))
                    }
                    (ScalarValue::Counter(_), _) => {
                        Err(FromCrdtError::Datastore("invalid: counter"))
                    }
                    (ScalarValue::Timestamp(_), Some(DefRepr::DateTime)) => todo!(),
                    (ScalarValue::Boolean(b), Some(DefRepr::Boolean)) => {
                        Ok(Attr::Unit(Value::I64(if *b { 1 } else { 0 }, tag)))
                    }
                    (ScalarValue::Unknown { .. }, _) => {
                        Err(FromCrdtError::Datastore("invalid: unknown"))
                    }
                    (ScalarValue::Null, Some(DefRepr::Unit)) => Ok(Attr::Unit(Value::unit())),
                    _ => Err(FromCrdtError::Datastore("invalid: null or unit")),
                }
            }
        }
    }

    fn deserialize_list(&self, obj_id: &ObjId, def: &Def) -> Result<Attr, FromCrdtError> {
        let mut column: Sequence<Value> = Default::default();

        for (am_value, obj_id) in self.automerge.values_at(obj_id, &self.heads) {
            let value = self.deserialize_value(&obj_id, am_value, def)?;
            column.push(value);
        }

        Ok(Attr::Matrix(AttrMatrix {
            columns: smallvec![column],
        }))
    }

    fn deserialize_map(&self, obj_id: &ObjId, def: &Def) -> Result<Value, FromCrdtError> {
        match def.repr() {
            Some(DefRepr::Struct) => {
                let mut attrs: FnvHashMap<PropId, Attr> = Default::default();

                for key in self.automerge.keys_at(obj_id, &self.heads) {
                    let prop_id = self.parse_am_property_name(&key)?;
                    let Some(rel_info) = def.data_relationships.get(&prop_id) else {
                        continue;
                    };

                    if let Some((am_value, obj_id)) =
                        self.automerge.get_at(obj_id, key, &self.heads)?
                    {
                        let attr_def = self.defs.def(rel_info.target.def_id());

                        let attr = self.deserialize_attr(&obj_id, am_value, attr_def)?;

                        attrs.insert(prop_id, attr);
                    }
                }

                Ok(Value::Struct(Box::new(attrs), def.id.into()))
            }
            _ => {
                todo!()
            }
        }
    }

    fn deserialize_text(&self, obj_id: &ObjId, def: &Def) -> Result<Value, FromCrdtError> {
        let text = self.automerge.text_at(obj_id, &self.heads)?;
        Ok(Value::Text(text.into(), def.id.into()))
    }

    fn parse_am_property_name(&self, prop_name: &str) -> Result<PropId, FromCrdtError> {
        let mut split = prop_name.split('_');
        let Some(domain_id) = split.next() else {
            return Err(FromCrdtError::Datastore("missing prop domain id"));
        };
        let Some(def_tag) = split.next() else {
            return Err(FromCrdtError::Datastore("missing prop def tag"));
        };
        let Some(prop_tag) = split.next() else {
            return Err(FromCrdtError::Datastore("missing prop tag"));
        };

        let domain_id = DomainId::from_str(domain_id).map_err(FromCrdtError::Datastore)?;

        let def_tag = u16::from_str(def_tag)
            .map_err(|_err| FromCrdtError::Datastore("invalid def tag format"))?;
        let prop_tag = u16::from_str(prop_tag)
            .map_err(|_err| FromCrdtError::Datastore("invalid prop tag format"))?;

        let Some(domain_index) = self.defs.domains().find_map(|(domain_index, domain)| {
            if domain.domain_id().id == domain_id {
                Some(domain_index)
            } else {
                None
            }
        }) else {
            return Err(FromCrdtError::Datastore("domain not in the ontology"));
        };

        Ok(PropId(DefId(domain_index, def_tag), DefPropTag(prop_tag)))
    }
}
