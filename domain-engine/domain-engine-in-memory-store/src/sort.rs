use std::cmp::Ordering;

use anyhow::anyhow;
use domain_engine_core::{DomainError, DomainResult};
use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    ontology::domain::EntityOrder,
    query::filter::Filter,
    value::{Attribute, Value},
    DefId, RelationshipId,
};

use crate::core::{DbContext, DynamicKey};

type Attrs = FnvHashMap<RelationshipId, Attribute>;

pub(super) fn sort_props_vec(
    raw_props_slice: &mut [(DynamicKey, FnvHashMap<RelationshipId, Attribute>)],
    def_id: DefId,
    filter: &Filter,
    ctx: &DbContext,
) -> DomainResult<()> {
    let order_symbols = filter.order();

    if order_symbols.is_empty() {
        return Ok(());
    }

    let info = ctx.ontology.extended_entity_info(def_id).unwrap();
    let entity_orders = order_symbols
        .iter()
        .map(|sym| info.order_table.get(&sym.type_def_id()).unwrap())
        .collect_vec();

    let mut cmp_error: Option<DomainError> = None;

    raw_props_slice.sort_by(
        |(_, a), (_, b)| match compare_order_tuple(a, b, &entity_orders) {
            Ok(ordering) => filter.direction().reorder(ordering),
            Err(error) => {
                cmp_error.get_or_insert(error);
                Ordering::Equal
            }
        },
    );

    if let Some(error) = cmp_error {
        Err(error)
    } else {
        Ok(())
    }
}

/// Compare by potentially a tuple of `order` relations
fn compare_order_tuple(
    a: &Attrs,
    b: &Attrs,
    entity_orders: &[&EntityOrder],
) -> DomainResult<Ordering> {
    for entity_order in entity_orders {
        match compare_entity_order(a, b, entity_order)? {
            Ordering::Equal => {
                continue;
            }
            unequal => return Ok(entity_order.direction.reorder(unequal)),
        }
    }

    Ok(Ordering::Equal)
}

/// Compare by one `order` relation (which can include a tuple of fields)
fn compare_entity_order(a: &Attrs, b: &Attrs, order: &EntityOrder) -> DomainResult<Ordering> {
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

fn value_by_path<'v>(attrs: &'v Attrs, path: &[RelationshipId]) -> DomainResult<&'v Value> {
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
