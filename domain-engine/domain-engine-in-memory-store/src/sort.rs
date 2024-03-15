use std::cmp::Ordering;

use anyhow::anyhow;
use domain_engine_core::{DomainError, DomainResult};
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::domain::EntityOrder,
    property::PropertyId,
    query::filter::Filter,
    value::{Attribute, Value},
    DefId,
};

use crate::core::{DbContext, DynamicKey};

type Attrs = FnvHashMap<PropertyId, Attribute>;

pub(super) fn sort_props_vec(
    raw_props_slice: &mut [(DynamicKey, FnvHashMap<PropertyId, Attribute>)],
    def_id: DefId,
    filter: &Filter,
    ctx: &DbContext,
) -> DomainResult<()> {
    let Some(order_symbol) = filter.order() else {
        return Ok(());
    };

    let info = ctx.ontology.extended_entity_info(def_id).unwrap();
    let entity_order = info.order_table.get(&order_symbol.type_def_id()).unwrap();
    let direction = entity_order.direction.chain(filter.direction());

    let mut cmp_error: Option<DomainError> = None;

    raw_props_slice.sort_by(|(_, a), (_, b)| match compare(a, b, entity_order) {
        Ok(ordering) => direction.reorder(ordering),
        Err(error) => {
            cmp_error.get_or_insert(error);
            Ordering::Equal
        }
    });

    if let Some(error) = cmp_error {
        Err(error)
    } else {
        Ok(())
    }
}

fn compare(a: &Attrs, b: &Attrs, order: &EntityOrder) -> DomainResult<Ordering> {
    for field_path in order.tuple.iter() {
        let first = value_by_path(a, &field_path.0)?;
        let second = value_by_path(b, &field_path.0)?;

        match first.partial_cmp(second) {
            None | Some(Ordering::Equal) => continue,
            Some(unequal) => return Ok(unequal),
        }
    }

    Ok(Ordering::Equal)
}

fn value_by_path<'v>(attrs: &'v Attrs, path: &[PropertyId]) -> DomainResult<&'v Value> {
    let property_id = path.first().unwrap();

    let attr = attrs
        .get(property_id)
        .ok_or_else(|| DomainError::DataStore(anyhow!("property not found")))?;

    if path.len() > 1 {
        match &attr.val {
            Value::Struct(sub_attrs, _) => value_by_path(sub_attrs, &path[1..]),
            _ => Err(DomainError::DataStore(anyhow!("not a struct"))),
        }
    } else {
        Ok(&attr.val)
    }
}
