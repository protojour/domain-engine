use std::cmp::Ordering;

use domain_engine_core::{DomainError, DomainResult};
use fnv::FnvHashMap;
use ontol_runtime::{
    DefId, PropId, attr::Attr, ontology::domain::VertexOrder, query::filter::Filter, value::Value,
};

use crate::core::{DbContext, DynamicKey};

type Attrs = FnvHashMap<PropId, Attr>;

pub(super) fn sort_props_vec(
    raw_props_slice: &mut [(&DynamicKey, FnvHashMap<PropId, Attr>)],
    _def_id: DefId,
    filter: &Filter,
    _ctx: &DbContext,
) -> DomainResult<()> {
    let vertex_order_tuple = filter
        .vertex_order()
        .ok_or_else(|| DomainError::data_store("vertex order not defined"))?;

    let mut cmp_error: Option<DomainError> = None;

    raw_props_slice.sort_by(
        |(_, a), (_, b)| match compare_order_tuple(a, b, vertex_order_tuple) {
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
    vertex_order_tuple: &[VertexOrder],
) -> DomainResult<Ordering> {
    for vertex_order in vertex_order_tuple {
        match compare_vertex_order(a, b, vertex_order)? {
            Ordering::Equal => {
                continue;
            }
            unequal => return Ok(vertex_order.direction.reorder(unequal)),
        }
    }

    Ok(Ordering::Equal)
}

/// Compare by one `order` relation (which can include a tuple of fields)
fn compare_vertex_order(a: &Attrs, b: &Attrs, order: &VertexOrder) -> DomainResult<Ordering> {
    for field_path in order.tuple.iter() {
        match (
            attr_by_path(a, &field_path.0),
            attr_by_path(b, &field_path.0),
        ) {
            (Ok(first), Ok(second)) => match first.partial_cmp(second) {
                None | Some(Ordering::Equal) => continue,
                Some(unequal) => return Ok(unequal),
            },
            _ => continue,
        }
    }

    Ok(Ordering::Equal)
}

fn attr_by_path<'v>(attrs: &'v Attrs, path: &[PropId]) -> DomainResult<&'v Attr> {
    let property_id = path.first().unwrap();

    let attr = attrs
        .get(property_id)
        .ok_or_else(|| DomainError::data_store("property not found"))?;

    if path.len() > 1 {
        match attr.as_unit() {
            Some(Value::Struct(sub_attrs, _)) => attr_by_path(sub_attrs, &path[1..]),
            _ => Err(DomainError::data_store("not a struct")),
        }
    } else {
        Ok(attr)
    }
}
