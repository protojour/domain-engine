use std::fmt::Debug;

use crate::{
    log_model::{ArcProp, DefProp, EKind, RelProp, UseProp},
    tag::Tag,
};

#[macro_export]
macro_rules! variant_fn {
    ($e:ident::$v:ident($d:ident)) => {
        |variant| {
            if let $e::$v($d) = variant {
                Some($d)
            } else {
                None
            }
        }
    };
    ($e:ident::$v:ident($d0:ident, $d1:ident)) => {
        |variant| {
            if let $e::$v($d0, $d1) = variant {
                Some(($d0, $d1))
            } else {
                None
            }
        }
    };
}

#[derive(Clone, Copy)]
pub enum Operation {
    Do,
    Undo,
}

pub fn traverse_reversible<T, E>(
    items: &[T],
    mut process: impl FnMut(Operation, usize, &T) -> Result<(), E>,
) -> Result<(), E>
where
    E: Debug,
{
    let mut traversed = 0;
    let mut error = None;

    for (idx, item) in items.iter().enumerate() {
        match process(Operation::Do, idx, item) {
            Ok(()) => {
                traversed = idx;
            }
            Err(err) => {
                error = Some(err);
                break;
            }
        }
    }

    if let Some(error) = error {
        for (idx, item) in items[0..traversed].iter().enumerate().rev() {
            process(Operation::Undo, idx, item).expect("undo error");
        }

        Err(error)
    } else {
        Ok(())
    }
}

pub fn history_use_props(history: &[EKind], tag: Tag) -> impl Iterator<Item = &UseProp> {
    history.iter().rev().flat_map(move |kind| match kind {
        EKind::UseAdd(t, props) | EKind::UseChange(t, props) if *t == tag => props.iter(),
        _ => [].iter(),
    })
}

pub fn history_def_props(history: &[EKind], tag: Tag) -> impl Iterator<Item = &DefProp> {
    history.iter().rev().flat_map(move |kind| match kind {
        EKind::DefAdd(t, props) | EKind::DefChange(t, props) if *t == tag => props.iter(),
        _ => [].iter(),
    })
}

pub fn history_arc_props(history: &[EKind], tag: Tag) -> impl Iterator<Item = &ArcProp> {
    history.iter().rev().flat_map(move |kind| match kind {
        EKind::ArcAdd(t, props) | EKind::ArcChange(t, props) if *t == tag => props.iter(),
        _ => [].iter(),
    })
}

pub fn history_rel_props(history: &[EKind], tag: Tag) -> impl Iterator<Item = &RelProp> {
    history.iter().rev().flat_map(move |kind| match kind {
        EKind::RelAdd(t, props) | EKind::RelChange(t, props) if *t == tag => props.iter(),
        _ => [].iter(),
    })
}
