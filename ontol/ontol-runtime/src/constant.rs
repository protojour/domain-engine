use tracing::debug;

use crate::{
    interface::serde::operator::{SerdeOperator, SerdeOperatorAddr},
    ontology::{
        aspects::{DefsAspect, SerdeAspect},
        domain::DefRepr,
    },
    value::Value,
    DefId,
};

pub fn try_produce_constant(
    def_id: DefId,
    ontology: &(impl AsRef<SerdeAspect> + AsRef<DefsAspect>),
) -> Option<Value> {
    let defs: &DefsAspect = ontology.as_ref();
    let def = defs.def(def_id);
    match def.repr() {
        Some(DefRepr::TextConstant(constant)) => {
            Some(Value::Text(defs[*constant].into(), def_id.into()))
        }
        _ => {
            let Some(operator_addr) = def.operator_addr else {
                debug!("{def_id:?} has no operator addr, no constant can be produced");
                return None;
            };
            try_produce_constant_from_operator(operator_addr, ontology)
        }
    }
}

fn try_produce_constant_from_operator(
    addr: SerdeOperatorAddr,
    ontology: &(impl AsRef<SerdeAspect> + AsRef<DefsAspect>),
) -> Option<Value> {
    let serde: &SerdeAspect = ontology.as_ref();
    let defs: &DefsAspect = ontology.as_ref();

    let operator = &serde.operators[addr.0 as usize];
    match operator {
        SerdeOperator::AnyPlaceholder => None,
        SerdeOperator::Unit => Some(Value::unit()),
        SerdeOperator::True(_) => Some(Value::boolean(true)),
        SerdeOperator::False(_) => Some(Value::boolean(false)),
        SerdeOperator::Boolean(_)
        | SerdeOperator::I32(..)
        | SerdeOperator::I64(..)
        | SerdeOperator::F64(..)
        | SerdeOperator::Serial(_)
        | SerdeOperator::Octets(_)
        | SerdeOperator::String(_) => None,
        SerdeOperator::StringConstant(text_constant, def_id) => {
            Some(Value::Text(defs[*text_constant].into(), (*def_id).into()))
        }
        SerdeOperator::TextPattern(_)
        | SerdeOperator::CapturingTextPattern(_)
        | SerdeOperator::DynamicSequence
        | SerdeOperator::RelationList(_)
        | SerdeOperator::RelationIndexSet(_)
        | SerdeOperator::ConstructorSequence(_) => None,
        SerdeOperator::Alias(alias_op) => {
            try_produce_constant_from_operator(alias_op.inner_addr, ontology)
        }
        SerdeOperator::Union(_) | SerdeOperator::Struct(_) => None,
        SerdeOperator::IdSingletonStruct(_, _, _) => None,
    }
}
