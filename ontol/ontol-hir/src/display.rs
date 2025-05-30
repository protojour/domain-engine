use std::fmt::{Debug, Display};

use ontol_runtime::{
    format_utils::AsAlpha,
    query::condition::{Clause, ClausePair},
    var::Var,
};
use thin_vec::ThinVec;

use crate::{
    Binding, CaptureGroup, EvalCondTerm, Kind, Label, Lang, MatrixRow, Node, OverloadFunc, Pack,
    PropVariant, RootNode, StructFlags,
    arena::{Arena, NodeRef},
};

impl<L: Lang> std::fmt::Display for NodeRef<'_, '_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let printer = Printer {
            indent: Sep::None,
            arena: self.arena,
        };
        printer.print(f, Sep::None, self.arena.kind_of(self.node))?;
        Ok(())
    }
}

impl<L: Lang> std::fmt::Display for RootNode<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<L: Lang> std::fmt::Debug for RootNode<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

type PrintResult = Result<Multiline, std::fmt::Error>;

pub trait Print<T>: Copy {
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, node: &T) -> PrintResult;
}

#[derive(Clone, Copy)]
pub struct Printer<'h, 'a, L: Lang> {
    indent: Sep,
    arena: &'h Arena<'a, L>,
}

impl<'h, 'a, L: Lang> Printer<'h, 'a, L> {
    fn kind(&self, node: Node) -> &Kind<'a, L> {
        self.arena.kind_of(node)
    }

    fn kinds<'n>(
        &self,
        nodes: &'n [Node],
    ) -> impl Iterator<Item = &'h Kind<'a, L>> + use<'n, 'h, 'a, L> {
        nodes.iter().map(|node| self.arena.kind_of(*node))
    }
}

impl<'a, L: Lang> Print<Kind<'a, L>> for Printer<'_, 'a, L> {
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, kind: &Kind<'a, L>) -> PrintResult {
        let indent = self.indent;
        match kind {
            Kind::NoOp => Ok(Multiline(false)),
            Kind::Var(var) => {
                write!(f, "{sep}{}", var)?;
                Ok(sep.multiline())
            }
            Kind::Block(nodes) => {
                write!(f, "{indent}(block")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(nodes))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Catch(label, nodes) => {
                write!(f, "{indent}(catch ({label})")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(nodes))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::CatchFunc(label, nodes) => {
                write!(f, "{indent}(catch-func ({label})")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(nodes))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Try(label, var) => {
                write!(f, "{indent}(try? {label} {var})")?;
                Ok(Multiline(false))
            }
            Kind::TryNarrow(try_label, var) => {
                write!(f, "{indent}(try-narrow? {try_label} {var})")?;
                Ok(Multiline(false))
            }
            Kind::Let(binder, node) => {
                write!(f, "{indent}(let {binder}", binder = L::as_hir(binder).var)?;
                let multi = self.print_all(f, Sep::Space, [self.kind(*node)].into_iter())?;
                self.print_rparen(f, multi)?;
                Ok(multi)
            }
            Kind::TryLet(try_label, binder, node) => {
                write!(
                    f,
                    "{indent}(let? {try_label} {binder}",
                    binder = L::as_hir(binder).var
                )?;
                let multi = self.print(f, Sep::Space, self.kind(*node))?;
                self.print_rparen(f, multi)?;
                Ok(multi)
            }
            Kind::LetProp(bind_pack, (var, prop_id)) => {
                write!(f, "{indent}(let-prop")?;
                self.print(f, Sep::Space, bind_pack)?;
                write!(f, " ({var} {prop_id})")?;
                self.print_rparen(f, Multiline(false))?;
                Ok(Multiline(true))
            }
            Kind::LetPropDefault(bind_pack, (var, prop_id), default) => {
                write!(f, "{indent}(let-prop-default")?;
                self.print(f, Sep::Space, bind_pack)?;
                write!(f, " ({var} {prop_id})")?;
                self.print_all(f, Sep::Space, self.kinds(default))?;
                self.print_rparen(f, Multiline(false))?;
                Ok(Multiline(true))
            }
            Kind::TryLetProp(try_label, bind_pack, (var, prop_id)) => {
                write!(f, "{indent}(let-prop? {try_label}")?;
                self.print(f, Sep::Space, bind_pack)?;
                write!(f, " ({var} {prop_id})")?;
                self.print_rparen(f, Multiline(false))?;
                Ok(Multiline(true))
            }
            Kind::TryLetTup(try_label, bindings, node) => {
                write!(f, "{indent}(let-tup? {try_label} (")?;
                self.print_all(f, Sep::None, bindings.iter())?;
                write!(f, ")")?;
                let multi = self.print(f, Sep::Space, self.kind(*node))?;
                self.print_rparen(f, multi)?;
                Ok(multi)
            }
            Kind::LetRegex(groups, regex_def_id, var) => {
                write!(f, "{indent}(let-regex")?;
                self.print_all(f, Sep::Space, groups.iter())?;
                write!(f, " {regex_def_id:?} {var}")?;
                self.print_rparen(f, Multiline(false))?;
                Ok(sep.multiline())
            }
            Kind::LetRegexIter(binder, groups, regex_def_id, var) => {
                write!(f, "{indent}(let-regex-iter {}", L::as_hir(binder).var)?;
                self.print_all(f, Sep::Space, groups.iter())?;
                write!(f, " {regex_def_id:?} {var}")?;
                self.print_rparen(f, Multiline(false))?;
                Ok(sep.multiline())
            }
            Kind::Unit => {
                write!(f, "{sep}#u")?;
                Ok(sep.multiline())
            }
            Kind::I64(int) => {
                write!(f, "{sep}{int}")?;
                Ok(sep.multiline())
            }
            Kind::F64(float) => {
                write!(f, "{sep}{float}")?;
                Ok(sep.multiline())
            }
            Kind::Text(string) => {
                write!(f, "{sep}'{string}'")?;
                Ok(sep.multiline())
            }
            Kind::Const(const_def_id) => {
                write!(f, "{sep}(const {const_def_id:?})")?;
                Ok(sep.multiline())
            }
            Kind::With(binder, definition, body) => {
                write!(f, "{indent}(with ({}", L::as_hir(binder).var)?;
                let multi = self.print(f, Sep::Space, self.kind(*definition))?;
                self.print_rparen(f, multi)?;
                let multi = self.print_all(f, self.indent.indent(), self.kinds(body))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Call(proc, args) => {
                let proc = match proc {
                    OverloadFunc::Add => "+",
                    OverloadFunc::Sub => "-",
                    OverloadFunc::Mul => "*",
                    OverloadFunc::Div => "/",
                    proc => panic!("unsupported proc {proc:?}"),
                };
                write!(f, "{sep}({proc}")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(args))?;
                self.print_rparen(f, multi)?;
                Ok(multi.or(sep))
            }
            Kind::Map(arg) => {
                write!(f, "{sep}(map")?;
                let multi = self.print_all(f, Sep::Space, [self.kind(*arg)].into_iter())?;
                self.print_rparen(f, multi)?;
                Ok(multi.or(sep))
            }
            Kind::Pun(arg) => {
                write!(f, "{sep}(pun")?;
                let multi = self.print_all(f, Sep::Space, [self.kind(*arg)].into_iter())?;
                self.print_rparen(f, multi)?;
                Ok(multi.or(sep))
            }
            Kind::Narrow(arg) => {
                write!(f, "{sep}(narrow")?;
                let multi = self.print_all(f, Sep::Space, [self.kind(*arg)].into_iter())?;
                self.print_rparen(f, multi)?;
                Ok(multi.or(sep))
            }
            Kind::Matrix(entries) => {
                write!(f, "{indent}(matrix")?;
                let multi = self.print_all(f, Sep::Space, entries.iter())?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Struct(binder, flags, children) => {
                if flags.contains(StructFlags::MATCH) {
                    write!(f, "{indent}(match-struct ({})", L::as_hir(binder).var)?;
                } else {
                    write!(f, "{indent}(struct ({})", L::as_hir(binder).var)?;
                }
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Prop(flags, struct_var, prop_id, variant) => {
                write!(f, "{indent}(prop{flags} {struct_var} {prop_id}",)?;
                let multi = self.print_all(f, Sep::Space, [variant].into_iter())?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::MoveRestAttrs(target, source) => {
                write!(f, "{sep}(move-rest-attrs {} {})", target, source)?;
                Ok(Multiline(false))
            }
            Kind::MakeSeq(binder, children) => {
                let indent = if children.is_empty() { sep } else { indent };
                write!(f, "{indent}(make-seq")?;
                if let Some(binder) = binder.as_ref() {
                    write!(f, " ({})", L::as_hir(binder).var)?;
                }
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(f, multi)?;
                Ok(multi)
            }
            Kind::MakeMatrix(binders, children) => {
                let indent = if children.is_empty() { sep } else { indent };
                write!(f, "{indent}(make-matrix (")?;
                {
                    let mut iter = binders.iter().peekable();
                    while let Some(binder) = iter.next() {
                        write!(f, "{}", L::as_hir(binder).var)?;
                        if iter.peek().is_some() {
                            write!(f, " ")?;
                        }
                    }
                }
                write!(f, ")")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(f, multi)?;
                Ok(multi)
            }
            Kind::CopySubSeq(target, source) => {
                write!(f, "{sep}(copy-sub-seq {} {})", target, source)?;
                Ok(Multiline(false))
            }
            Kind::ForEach(elements, children) => {
                write!(f, "{indent}(for-each ")?;
                for (var, _) in elements {
                    write!(f, "{var} ")?;
                }

                {
                    write!(f, "(")?;
                    let mut iter = elements.iter().peekable();
                    while let Some((_, binding)) = iter.next() {
                        write!(f, "{binding}")?;
                        if iter.peek().is_some() {
                            write!(f, " ")?;
                        }
                    }
                    write!(f, ")")?;
                }
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Insert(seq_var, node) => {
                write!(f, "{indent}(insert {seq_var}",)?;
                let multi = self.print_all(f, Sep::Space, self.kinds(&[*node]))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::StringPush(to_var, node) => {
                write!(f, "{indent}(string-push {to_var}",)?;
                let multi = self.print(f, Sep::Space, self.kind(*node))?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Kind::Regex(label, regex_def_id, captures) => {
                if let Some(label) = label {
                    write!(f, "{sep}(regex-seq ({}) {regex_def_id:?}", L::as_hir(label))?;
                } else {
                    write!(f, "{sep}(regex {regex_def_id:?}")?;
                }
                let multi = self.print_all(f, Sep::Space, captures.iter())?;
                self.print_rparen(f, multi)?;
                Ok(sep.multiline())
            }
            Kind::LetCondVar(binder_var, cond) => {
                write!(f, "{indent}(let-cond-var {binder_var} {cond})")?;
                Ok(Multiline(false))
            }
            Kind::PushCondClauses(var, clauses) => {
                write!(f, "{indent}(push-cond-clauses {var}")?;
                self.print_all(f, Sep::Space, clauses.iter())?;
                self.print_rparen(f, Multiline(true))?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<'a, L: Lang> Print<Pack<Binding<'a, L>>> for Printer<'_, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        sep: Sep,
        pack: &Pack<Binding<'a, L>>,
    ) -> PrintResult {
        write!(f, "{sep}")?;

        match pack {
            Pack::Unit(u) => {
                self.print(f, Sep::None, u)?;
                Ok(Multiline(false))
            }
            Pack::Tuple(t) => {
                write!(f, "[")?;
                let multi = self.print_all(f, Sep::None, t.iter())?;
                self.print_rbracket(f, multi)?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<L: Lang> Print<PropVariant> for Printer<'_, '_, L> {
    fn print(self, f: &mut std::fmt::Formatter, _sep: Sep, variant: &PropVariant) -> PrintResult {
        let indent = self.indent;

        match variant {
            PropVariant::Unit(node) => {
                let multi = self.print(f, Sep::Space, self.kind(*node))?;
                Ok(multi)
            }
            PropVariant::Tuple(tup) => {
                write!(f, "{indent}[")?;
                let multi = self.print_all(f, Sep::None, self.kinds(tup))?;
                self.print_rbracket(f, multi)?;
                Ok(Multiline(true))
            }
            PropVariant::Predicate(operator, param) => {
                write!(f, "{indent}(")?;
                write!(f, "{operator}")?;
                let multi = self.print(f, Sep::Space, self.kind(*param))?;
                self.print_rparen(f, multi)?;
                Ok(multi)
            }
        }
    }
}

impl<L: Lang> Print<ClausePair<Var, EvalCondTerm>> for Printer<'_, '_, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        clause: &ClausePair<Var, EvalCondTerm>,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}")?;

        let var = clause.0;
        match &clause.1 {
            Clause::Root => {
                write!(f, "(root '{var})")?;
                Ok(Multiline(true))
            }
            Clause::IsDef(def_id) => {
                write!(f, "(is-def {var} {def_id:?})")?;
                Ok(Multiline(true))
            }
            Clause::MatchProp(prop_id, operator, set_var) => {
                write!(f, "(match-prop '{var} {prop_id} {operator} '{set_var}")?;
                self.print_rparen(f, Multiline(false))?;
                Ok(Multiline(true))
            }
            Clause::Member(rel, val) => {
                write!(f, "(member '{var} (")?;
                let multi = self.print_all(f, Sep::None, [rel, val].into_iter())?;
                self.print_rparen(f, multi)?;
                self.print_rparen(f, multi)?;
                Ok(Multiline(true))
            }
            Clause::SetPredicate(..) => {
                write!(f, "(set-predicate)")?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<L: Lang> Print<EvalCondTerm> for Printer<'_, '_, L> {
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, term: &EvalCondTerm) -> PrintResult {
        // let indent = self.indent;
        // write!(f, "{indent}")?;
        match term {
            EvalCondTerm::Wildcard => {
                write!(f, "{sep}_")?;
                Ok(Multiline(false))
            }
            EvalCondTerm::QuoteVar(var) => {
                write!(f, "{sep}'{var}")?;
                Ok(Multiline(false))
            }
            EvalCondTerm::Eval(node) => self.print(f, sep, self.kind(*node)),
        }
    }
}

impl<'a, L: Lang> Print<Binding<'a, L>> for Printer<'_, 'a, L> {
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, ast: &Binding<'a, L>) -> PrintResult {
        match ast {
            Binding::Binder(binder) => {
                write!(f, "{sep}{}", L::as_hir(binder).var)?;
            }
            Binding::Wildcard => {
                write!(f, "{sep}$_")?;
            }
        }
        Ok(Multiline(false))
    }
}

impl<'a, L: Lang> Print<CaptureGroup<'a, L>> for Printer<'_, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        sep: Sep,
        group: &CaptureGroup<'a, L>,
    ) -> PrintResult {
        write!(f, "{sep}({} {})", group.index, L::as_hir(&group.binder).var)?;
        Ok(Multiline(false))
    }
}

impl<'a, L: Lang> Print<MatrixRow<'a, L>> for Printer<'_, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        MatrixRow(iter_label, elements): &MatrixRow<'a, L>,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        if let Some(iter_label) = iter_label {
            write!(f, ".. {}", L::as_hir(iter_label))?;
        }
        let multi = self.print_all(f, Sep::Space, self.kinds(elements))?;
        self.print_rparen(f, multi)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang, T> Print<Vec<T>> for Printer<'h, 'a, L>
where
    Printer<'h, 'a, L>: Print<T>,
{
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, node: &Vec<T>) -> PrintResult {
        write!(f, "{sep}(")?;
        let multi = self.print_all(f, Sep::None, node.iter())?;
        self.print_rparen(f, multi)?;
        Ok(multi)
    }
}

impl<'h, 'a, L: Lang, T> Print<ThinVec<T>> for Printer<'h, 'a, L>
where
    Printer<'h, 'a, L>: Print<T>,
{
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, node: &ThinVec<T>) -> PrintResult {
        write!(f, "{sep}(")?;
        let multi = self.print_all(f, Sep::None, node.iter())?;
        self.print_rparen(f, multi)?;
        Ok(multi)
    }
}

impl<L: Lang> Printer<'_, '_, L> {
    fn print_rparen(self, f: &mut std::fmt::Formatter, multi: Multiline) -> std::fmt::Result {
        if multi.0 {
            let indent = self.indent.force_indent();
            write!(f, "{indent})")?;
        } else {
            write!(f, ")")?;
        }
        Ok(())
    }

    fn print_rbracket(self, f: &mut std::fmt::Formatter, multi: Multiline) -> std::fmt::Result {
        if multi.0 {
            let indent = self.indent.force_indent();
            write!(f, "{indent}]")?;
        } else {
            write!(f, "]")?;
        }
        Ok(())
    }

    fn print_all<'b, T: 'b, I>(
        self,
        f: &mut std::fmt::Formatter,
        mut sep: Sep,
        asts: I,
    ) -> PrintResult
    where
        Self: Print<T>,
        I: Iterator<Item = &'b T>,
    {
        let mut printer = self;
        printer.indent = self.indent.indent();
        let mut multiline = Multiline(false);
        for ast in asts {
            let last_multiline = printer.print(f, sep, ast)?;
            if last_multiline.0 {
                sep = printer.indent;
                multiline = last_multiline;
            } else {
                sep = Sep::Space;
            }
        }
        Ok(multiline)
    }
}

#[derive(Clone, Copy)]
pub struct Multiline(bool);

impl Multiline {
    fn or(self, sep: Sep) -> Self {
        if self.0 {
            return self;
        }
        match sep {
            Sep::Indent(_) => Self(true),
            _ => Self(false),
        }
    }
}

#[derive(Clone, Copy)]
pub enum Sep {
    None,
    Space,
    Indent(usize),
}

impl Sep {
    fn multiline(self) -> Multiline {
        if matches!(self, Sep::Indent(_)) {
            Multiline(true)
        } else {
            Multiline(false)
        }
    }

    fn indent(self) -> Self {
        match self {
            Self::None => Self::Indent(1),
            Self::Space => Self::Space,
            Self::Indent(level) => Self::Indent(level + 1),
        }
    }

    fn force_indent(self) -> Self {
        match self {
            Self::None | Self::Space => Self::Indent(0),
            Self::Indent(level) => Self::Indent(level),
        }
    }
}

impl std::fmt::Display for Sep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => {}
            Self::Space => {
                write!(f, " ")?;
            }
            Self::Indent(indent) => {
                write!(f, "{}", Indent(*indent))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Indent(usize);

impl std::fmt::Display for Indent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f)?;
        for _ in 0..self.0 {
            write!(f, "    ")?;
        }
        Ok(())
    }
}

impl<L: Lang> Display for Binding<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wildcard => write!(f, "$_"),
            Self::Binder(binder) => write!(f, "{}", L::as_hir(binder).var),
        }
    }
}

impl Debug for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Label({})", AsAlpha(self.0, 'a'))
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}", AsAlpha(self.0, 'a'))
    }
}
