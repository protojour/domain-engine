use std::{
    collections::BTreeSet,
    hash::{Hash, Hasher},
};

use fnv::FnvHashMap;
use itertools::{EitherOrBoth, Itertools};
use ontol_parser::cst::view::TypedView;
use ontol_syntax::{
    OntolLang,
    rowan::{self, GreenNodeData, GreenTokenData, Language, NodeOrToken},
    syntax_view::RowanNodeView,
};
use rustc_hash::FxHasher;
use smallvec::SmallVec;

#[derive(Debug)]
pub enum DiffError {
    InvalidRoot,
    InvalidSyntax,
    InvalidChange,
}

pub struct DiffOutput<T, C> {
    pub added: Vec<T>,
    pub changed: Vec<C>,
    pub removed: Vec<T>,
}

pub enum Change<T, C> {
    Distinct(T, T),
    Interior(C),
}

impl<T, C> DiffOutput<T, C> {
    fn added_ch_rm_counts(&self) -> (usize, usize, usize) {
        (self.added.len(), self.changed.len(), self.removed.len())
    }
}

pub fn diff<T: TypedView<RowanNodeView>, C>(
    iter: impl Iterator<Item = T>,
    prev_iter: impl Iterator<Item = T>,
    pair_fn: impl Fn(T, T) -> Option<Change<T, C>>,
) -> Result<DiffOutput<T, C>, DiffError> {
    let mut iter = iter.map(TypedView::into_view).peekable();
    let mut prev_iter = prev_iter.map(TypedView::into_view).peekable();

    let mut diff_output: DiffOutput<T, C> = DiffOutput {
        added: vec![],
        changed: vec![],
        removed: vec![],
    };

    while let Some(node) = iter.peek() {
        if let Some(prev) = prev_iter.peek() {
            if ModuloTrivia(node.syntax().green().as_ref())
                == ModuloTrivia(prev.syntax().green().as_ref())
            {
                // identical (modulo "trivia"), onto the next one:
                iter.next();
                prev_iter.next();
                continue;
            }
        }

        // mismatch detected
        break;
    }

    if iter.peek().is_none() && prev_iter.peek().is_none() {
        // "fast-mode" done, no changes
        return Ok(diff_output);
    }

    let mut cur_pushback: Vec<RowanNodeView> = vec![];
    let mut prev_set = PrevSet::new(prev_iter);

    for node in iter {
        if prev_set.find_remove(&node).is_none() {
            cur_pushback.push(node);
        }
    }

    if !cur_pushback.is_empty() {
        // cross-pairing attempts O(N*M) of remaining un-matched
        for node in std::mem::take(&mut cur_pushback) {
            let kind = node.syntax().kind();

            let mut cur_typed = T::from_view(node).ok_or(DiffError::InvalidSyntax)?;

            let mut remaining = prev_set.remaining();

            let paired = loop {
                if let Some((index, prev)) = remaining.next() {
                    // Can't match incompatible syntax nodes
                    if prev.syntax().kind() != kind {
                        continue;
                    }

                    let prev_typed = T::from_view(prev.clone()).ok_or(DiffError::InvalidSyntax)?;

                    match pair_fn(cur_typed, prev_typed) {
                        Some(Change::Distinct(c, _p)) => {
                            cur_typed = c;
                        }
                        Some(Change::Interior(change)) => {
                            diff_output.changed.push(change);
                            break Ok(index);
                        }
                        None => {
                            return Err(DiffError::InvalidChange);
                        }
                    }
                } else {
                    break Err(cur_typed);
                }
            };

            drop(remaining);

            match paired {
                Ok(paired_index) => {
                    prev_set.remaining.remove(&paired_index);
                }
                Err(unpaired_cur) => {
                    diff_output.added.push(unpaired_cur);
                }
            }
        }
    }

    diff_output.removed = prev_set
        .into_remaining()
        .map(|view| T::from_view(view).ok_or(DiffError::InvalidSyntax))
        .collect::<Result<_, _>>()?;

    Ok(diff_output)
}

struct PrevSet {
    original: Vec<RowanNodeView>,
    remaining: BTreeSet<usize>,
    hashes: FnvHashMap<u64, SmallVec<usize, 1>>,
}

impl PrevSet {
    fn new(iter: impl Iterator<Item = RowanNodeView>) -> Self {
        let original: Vec<_> = iter.collect();
        let remaining: BTreeSet<usize> = (0..(original.len())).collect();
        let mut hashes: FnvHashMap<u64, SmallVec<usize, 1>> = FnvHashMap::default();

        for (index, prev) in original.iter().enumerate() {
            hashes
                .entry(hash_modulo_trivia(prev.syntax().green().as_ref()))
                .or_default()
                .push(index);
        }

        Self {
            original,
            remaining,
            hashes,
        }
    }

    fn find_remove(&mut self, matching: &RowanNodeView) -> Option<()> {
        let hash = hash_modulo_trivia(matching.syntax().green().as_ref());
        for index in self.hashes.get(&hash)? {
            if !self.remaining.contains(index) {
                continue;
            }

            let node = &self.original[*index];

            if ModuloTrivia(node.syntax().green().as_ref())
                == ModuloTrivia(matching.syntax().green().as_ref())
            {
                self.remaining.remove(index);
                return Some(());
            }
        }

        None
    }

    fn remaning_len(&self) -> usize {
        self.remaining.len()
    }

    fn remaining(&self) -> impl Iterator<Item = (usize, &RowanNodeView)> {
        self.original
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                if self.remaining.contains(&index) {
                    Some((index, item))
                } else {
                    None
                }
            })
    }

    fn into_remaining(self) -> impl Iterator<Item = RowanNodeView> {
        let original = self.original;
        let mut remaining = self.remaining;

        original
            .into_iter()
            .enumerate()
            .filter_map(move |(index, item)| {
                if remaining.remove(&index) {
                    Some(item)
                } else {
                    None
                }
            })
    }
}

pub fn eq_modulo_trivia_view(lhs: &RowanNodeView, rhs: &RowanNodeView) -> bool {
    ModuloTrivia(lhs.syntax().green().as_ref()) == ModuloTrivia(rhs.syntax().green().as_ref())
}

fn hash_modulo_trivia(data: &GreenNodeData) -> u64 {
    let mut h = FxHasher::default();
    ModuloTrivia(data).hash(&mut h);
    h.finish()
}

/// A green node context that ignores trivia (whitespace, comments)
/// when comparing and hashing
pub struct ModuloTrivia<'a>(&'a GreenNodeData);

impl Hash for ModuloTrivia<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.kind().hash(state);

        for child in non_trivia_children(&self.0) {
            match child {
                rowan::NodeOrToken::Node(node) => ModuloTrivia(node).hash(state),
                rowan::NodeOrToken::Token(token) => {
                    let kind = OntolLang::kind_from_raw(token.kind());

                    if !kind.is_trivia() {
                        token.kind().hash(state);
                        token.text().hash(state);
                    }
                }
            }
        }
    }
}

impl<'a> PartialEq<ModuloTrivia<'a>> for ModuloTrivia<'a> {
    fn eq(&self, other: &ModuloTrivia) -> bool {
        if self.0.kind() != other.0.kind() {
            return false;
        }

        for item in non_trivia_children(&self.0).zip_longest(non_trivia_children(&other.0)) {
            match item {
                EitherOrBoth::Both(NodeOrToken::Node(lhs), NodeOrToken::Node(rhs)) => {
                    if ModuloTrivia(lhs) != ModuloTrivia(rhs) {
                        return false;
                    }
                }
                EitherOrBoth::Both(NodeOrToken::Token(lhs), NodeOrToken::Token(rhs)) => {
                    if lhs.kind() != rhs.kind() || lhs.text() != rhs.text() {
                        return false;
                    }
                }
                _ => return false,
            }
        }

        true
    }
}

fn non_trivia_children(
    node: &GreenNodeData,
) -> impl Iterator<Item = NodeOrToken<&GreenNodeData, &GreenTokenData>> {
    node.children()
        .filter(|child| !OntolLang::kind_from_raw(child.kind()).is_trivia())
}

#[cfg(test)]
mod tests {
    use ontol_parser::cst::{
        grammar,
        inspect::{Ontol, Statement},
    };
    use ontol_syntax::parse_syntax;

    use super::*;

    type Pair<T> = (T, T);

    fn parse(src: &str) -> Ontol<RowanNodeView> {
        let (root, errors) = parse_syntax(src, grammar::ontol, None);
        assert!(errors.is_empty());

        Ontol::from_view(RowanNodeView(root)).unwrap()
    }

    fn diff_ontol(
        a: &str,
        b: &str,
        pair_fn: impl Fn(
            Statement<RowanNodeView>,
            Statement<RowanNodeView>,
        ) -> Option<Change<Statement<RowanNodeView>, ()>>,
    ) -> DiffOutput<Statement<RowanNodeView>, ()> {
        let root1 = parse(a);
        let root2 = parse(b);

        diff::<Statement<_>, ()>(root1.statements(), root2.statements(), pair_fn).unwrap()
    }

    #[test]
    fn test_no_change() {
        let src = "def foo () def bar ()";
        let output = diff_ontol(src, src, |_, _| panic!());
        assert_eq!((0, 0, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_trivia_change() {
        let old = "def foo ( )";
        let new = "def foo ()";

        let output = diff_ontol(new, old, |_, _| panic!());
        assert_eq!((0, 0, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_add_stmt_end() {
        let old = "def foo ()";
        let new = "def foo () def bar ()";

        let output = diff_ontol(new, old, |_, _| panic!());

        assert_eq!((1, 0, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_add_stmt_start() {
        let old = "           def foo ()";
        let new = "def bar () def foo ()";

        let output = diff_ontol(new, old, |_, _| panic!());
        assert_eq!((1, 0, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_change_stmt_start() {
        let old = "def foo () def bar ()";
        let new = "def baz () def bar ()";

        let output = diff_ontol(new, old, |_, _| Some(Change::Interior(())));
        assert_eq!((0, 1, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_change_stmt_end() {
        let old = "def foo () def bar ()";
        let new = "def foo () def baz ()";

        let output = diff_ontol(new, old, |_, _| Some(Change::Interior(())));
        assert_eq!((0, 1, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_change_2_stmts() {
        let old = "def foo () def bar ()";
        let new = "def baz () def qux ()";

        let output = diff_ontol(new, old, |_, _| Some(Change::Interior(())));
        assert_eq!((0, 2, 0), output.added_ch_rm_counts());
    }

    #[test]
    fn test_rm_stmt_start() {
        let old = "def foo () def bar ()";
        let new = "           def bar ()";

        let output = diff_ontol(new, old, |_, _| panic!());
        assert_eq!((0, 0, 1), output.added_ch_rm_counts());
    }

    #[test]
    fn test_rm_stmt_end() {
        let old = "def foo () def bar ()";
        let new = "def foo ()           ";

        let output = diff_ontol(new, old, |_, _| panic!());
        assert_eq!((0, 0, 1), output.added_ch_rm_counts());
    }
}
