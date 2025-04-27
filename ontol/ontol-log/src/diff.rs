use std::collections::BTreeMap;

use either::Either;

use crate::{
    log_model::{Pattern, TypeRef, TypeRefOrUnionOrPattern, TypeUnion},
    with_span::WithSpan,
};

/// Returns current if not equal to origin
#[inline]
pub fn cmp_opt2<'a, T: Eq>(current: &'a Option<T>, origin: Option<&T>) -> Option<&'a T> {
    match (current, origin) {
        (Some(current), Some(origin)) => {
            if current != origin {
                Some(current)
            } else {
                None
            }
        }
        (Some(current), None) => Some(current),
        (None, _) => None,
    }
}

pub fn diff_type_ref_or_union2(
    new: Either<&WithSpan<TypeRef>, &TypeUnion>,
    old: Either<&WithSpan<TypeRef>, &TypeUnion>,
    mut f: impl FnMut(bool, &WithSpan<TypeRef>),
) {
    match (new, old) {
        (Either::Left(new), Either::Left(old)) => {
            if new != old {
                f(false, old);
                f(true, new);
            }
        }
        (Either::Left(new), Either::Right(old)) => {
            let mut yield_new = true;
            for old in old {
                if old == new {
                    yield_new = false;
                } else {
                    f(false, old);
                }
            }
            if yield_new {
                f(true, new);
            }
        }
        (Either::Right(new), Either::Left(old)) => {
            if !new.contains(old) {
                f(false, old);
            }
            for new in new {
                if new != old {
                    f(true, new);
                }
            }
        }
        (Either::Right(new), Either::Right(old)) => {
            for t in old.difference(new) {
                f(false, t);
            }

            for t in new.difference(old) {
                f(true, t);
            }
        }
    }
}

pub fn diff_type_ref_or_union_or_pattern(
    new: &TypeRefOrUnionOrPattern,
    old: &TypeRefOrUnionOrPattern,
    mut f: impl FnMut(bool, Either<&WithSpan<TypeRef>, &WithSpan<Pattern>>),
) {
    match (new.to_either_ref(), old.to_either_ref()) {
        (Either::Left(new), Either::Left(old)) => {
            diff_type_ref_or_union2(new, old, |added, t| {
                if added {
                    f(true, Either::Left(t));
                } else {
                    f(false, Either::Left(t));
                }
            });
        }
        (Either::Left(new), Either::Right(old)) => {
            f(false, Either::Right(old));
            for new in new.map_left(Some).into_iter() {
                f(true, Either::Left(new));
            }
        }
        (Either::Right(new), Either::Left(old)) => {
            for old in old.map_left(Some).into_iter() {
                f(false, Either::Left(old));
            }
            f(true, Either::Right(new));
        }
        (Either::Right(new), Either::Right(old)) => {
            if new != old {
                f(false, Either::Right(old));
                f(true, Either::Right(new));
            }
        }
    }
}

pub fn diff_btree_maps<K, V>(
    new: &BTreeMap<K, V>,
    old: &BTreeMap<K, V>,
    mut f: impl FnMut(bool, &K, &V),
) where
    K: Eq + Ord,
    V: Eq + Ord,
{
    let mut new = new.iter().peekable();
    let mut old = old.iter().peekable();

    loop {
        let Some(new_e) = new.peek() else {
            break;
        };
        let Some(old_e) = old.peek() else {
            break;
        };

        if old_e < new_e {
            f(false, old_e.0, old_e.1);
            old.next();
        } else if old_e > new_e {
            f(true, new_e.0, new_e.1);
            new.next();
        } else {
            new.next();
            old.next();
        }
    }

    for (old_k, old_v) in old {
        f(false, old_k, old_v);
    }

    for (new_k, new_v) in new {
        f(true, new_k, new_v);
    }
}
