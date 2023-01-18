use std::ops::Range;

use chumsky::prelude::Simple;
use smartstring::{LazyCompact, SmartString};

use self::tree::Tree;

pub mod ast;
pub mod tree;

mod tree_stream;

pub type SString = SmartString<LazyCompact>;
pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
