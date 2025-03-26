use either::Either;

use crate::log_model::{Pattern, TypeRef, TypeRefOrUnionOrPattern, TypeUnion};

pub fn diff_type_ref_or_union(
    new: Either<TypeRef, TypeUnion>,
    old: Either<TypeRef, TypeUnion>,
    mut f: impl FnMut(bool, &TypeRef),
) {
    let new = new.right_or_else(|t| [t].into_iter().collect());
    let old = old.right_or_else(|t| [t].into_iter().collect());

    for t in old.difference(&new) {
        f(false, t);
    }

    for t in new.difference(&old) {
        f(true, t);
    }
}

pub fn diff_type_ref_or_union_or_pattern(
    new: TypeRefOrUnionOrPattern,
    old: TypeRefOrUnionOrPattern,
    mut f: impl FnMut(bool, Either<&TypeRef, Pattern>),
) {
    match (new.to_type_set(), old.to_type_set()) {
        (Either::Left(new), Either::Left(old)) => {
            for t in old.difference(&new) {
                f(false, Either::Left(t));
            }

            for t in new.difference(&old) {
                f(true, Either::Left(t));
            }
        }
        (Either::Right(new_pat), Either::Right(old_pat)) => {
            if new_pat != old_pat {
                f(false, Either::Right(old_pat));
                f(true, Either::Right(new_pat));
            }
        }
        (Either::Left(new_union), Either::Right(old_pat)) => {
            f(false, Either::Right(old_pat));
            for t in &new_union {
                f(true, Either::Left(t));
            }
        }
        (Either::Right(new_pat), Either::Left(old_union)) => {
            for t in &old_union {
                f(false, Either::Left(t));
            }
            f(true, Either::Right(new_pat));
        }
    }
}
