use std::fmt::{Debug, Display};

use ontol_runtime::{condition::Clause, format_utils::AsAlpha, vm::proc::BuiltinProc};

use crate::{
    arena::{Arena, NodeRef},
    Attribute, Binding, CaptureGroup, CaptureMatchArm, EvalCondTerm, HasDefault, Iter, Kind, Label,
    Lang, Node, Nodes, PredicateClosure, PropPattern, PropVariant, RootNode, SetEntry, StructFlags,
};

impl<'h, 'a, L: Lang> std::fmt::Display for NodeRef<'h, 'a, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let printer = Printer {
            indent: Sep::None,
            arena: self.arena,
        };
        printer.print(f, Sep::None, self.arena.kind_of(self.node))?;
        Ok(())
    }
}

impl<'a, L: Lang> std::fmt::Display for RootNode<'a, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<'a, L: Lang> std::fmt::Debug for RootNode<'a, L> {
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

    fn kinds<'n>(&self, nodes: &'n [Node]) -> impl Iterator<Item = &'h Kind<'a, L>> + 'n
    where
        'h: 'n,
    {
        nodes.iter().map(|node| self.arena.kind_of(*node))
    }
}

impl<'h, 'a, L: Lang> Print<Kind<'a, L>> for Printer<'h, 'a, L> {
    fn print(self, f: &mut std::fmt::Formatter, sep: Sep, kind: &Kind<'a, L>) -> PrintResult {
        let indent = self.indent;
        match kind {
            Kind::Var(var) => {
                write!(f, "{sep}{}", var)?;
                Ok(sep.multiline())
            }
            Kind::Begin(nodes) => {
                write!(f, "{indent}(begin")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(nodes))?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
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
            Kind::Let(binder, definition, body) => {
                write!(f, "{indent}(let ({}", L::as_hir(binder).var)?;
                let multi = self.print(f, Sep::Space, self.kind(*definition))?;
                self.print_rparen(multi, f)?;
                let multi = self.print_all(f, self.indent.indent(), self.kinds(body))?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Call(proc, args) => {
                let proc = match proc {
                    BuiltinProc::Add => "+",
                    BuiltinProc::Sub => "-",
                    BuiltinProc::Mul => "*",
                    BuiltinProc::Div => "/",
                    proc => panic!("unsupported proc {proc:?}"),
                };
                write!(f, "{sep}({proc}")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(args))?;
                self.print_rparen(multi, f)?;
                Ok(multi.or(sep))
            }
            Kind::Map(arg) => {
                write!(f, "{sep}(map")?;
                let multi = self.print_all(f, Sep::Space, [self.kind(*arg)].into_iter())?;
                self.print_rparen(multi, f)?;
                Ok(multi.or(sep))
            }
            Kind::Set(entries) => {
                write!(f, "{indent}(set")?;
                let multi = self.print_all(f, Sep::Space, entries.iter())?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Struct(binder, flags, children) => {
                if flags.contains(StructFlags::MATCH) {
                    write!(f, "{indent}(match-struct ({})", L::as_hir(binder).var)?;
                } else {
                    write!(f, "{indent}(struct ({})", L::as_hir(binder).var)?;
                }
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Prop(optional, struct_var, prop_id, variants) => {
                write!(
                    f,
                    "{indent}(prop{} {struct_var} {prop_id}",
                    if optional.0 { "?" } else { "" }
                )?;
                let multi = self.print_all(f, Sep::Space, variants.iter())?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::MoveRestAttrs(target, source) => {
                write!(f, "{sep}(move-rest-attrs {} {})", target, source)?;
                Ok(Multiline(false))
            }
            Kind::MatchProp(struct_var, id, arms) => {
                write!(f, "{indent}(match-prop {struct_var} {id}")?;
                let multi = self.print_all(f, Sep::Space, arms.iter())?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::MakeSeq(binder, children) => {
                let indent = if children.is_empty() { sep } else { indent };
                write!(f, "{indent}(make-seq ({})", L::as_hir(binder).var)?;
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(multi, f)?;
                Ok(multi)
            }
            Kind::CopySubSeq(target, source) => {
                write!(f, "{sep}(copy-sub-seq {} {})", target, source)?;
                Ok(Multiline(false))
            }
            Kind::ForEach(var, (rel, val), children) => {
                write!(f, "{indent}(for-each {var} ({rel} {val})")?;
                let multi = self.print_all(f, Sep::Space, self.kinds(children))?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Insert(seq_var, attr) => {
                write!(f, "{indent}(insert {seq_var}",)?;
                let multi = self.print_all(f, Sep::Space, self.kinds(&[attr.rel, attr.val]))?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::StringPush(to_var, node) => {
                write!(f, "{indent}(string-push {to_var}",)?;
                let multi = self.print(f, Sep::Space, self.kind(*node))?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Regex(label, regex_def_id, captures) => {
                if let Some(label) = label {
                    write!(f, "{sep}(regex-seq ({}) {regex_def_id:?}", L::as_hir(label))?;
                } else {
                    write!(f, "{sep}(regex {regex_def_id:?}")?;
                }
                let multi = self.print_all(f, Sep::Space, captures.iter())?;
                self.print_rparen(multi, f)?;
                Ok(sep.multiline())
            }
            Kind::MatchRegex(iter, var, regex_def_id, arms) => {
                if iter.0 {
                    write!(f, "{indent}(match-regex-iter")?;
                } else {
                    write!(f, "{indent}(match-regex")?;
                }
                write!(f, " {var} {regex_def_id:?}")?;
                let multi = self.print_all(f, Sep::Space, arms.iter())?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::PushCondClause(var, clause) => {
                write!(f, "{indent}(push-cond-clause {var}")?;
                let multi = self.print_all(f, self.indent.indent(), [clause].into_iter())?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<'h, 'a, L: Lang> Print<PropVariant<'a, L>> for Printer<'h, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        variant: &PropVariant<'a, L>,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;

        let multi = match variant {
            PropVariant::Singleton(attr) => {
                self.print_all(f, Sep::None, self.kinds(&[attr.rel, attr.val]))?
            }
            PropVariant::Set(seq_variant) => {
                if seq_variant.has_default.0 {
                    write!(f, "..default")?;
                } else {
                    write!(f, "..")?;
                }
                write!(f, " ({})", L::as_hir(&seq_variant.label))?;

                self.print_all(f, Sep::Space, seq_variant.elements.iter())?;
                Multiline(true)
            }
            PropVariant::Predicate(closure) => match closure {
                PredicateClosure::ContainsElement(attr) => {
                    write!(f, "contains-element")?;
                    self.print_all(f, Sep::Space, self.kinds(&[attr.rel, attr.val]))?
                }
                PredicateClosure::ElementIn(node) => {
                    write!(f, "element-in")?;
                    self.print(f, Sep::Space, self.kind(*node))?
                }
                PredicateClosure::AllInSet(node) => {
                    write!(f, "all-in-set")?;
                    self.print(f, Sep::Space, self.kind(*node))?
                }
                PredicateClosure::SetContainsAll(node) => {
                    write!(f, "set-contains-all")?;
                    self.print(f, Sep::Space, self.kind(*node))?
                }
                PredicateClosure::SetIntersects(node) => {
                    write!(f, "set-intersects")?;
                    self.print(f, Sep::Space, self.kind(*node))?
                }
                PredicateClosure::SetEquals(node) => {
                    write!(f, "set-equals")?;
                    self.print(f, Sep::Space, self.kind(*node))?
                }
            },
        };

        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<(Iter, Attribute<Node>)> for Printer<'h, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        (iter, attr): &(Iter, Attribute<Node>),
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;

        let sep = if iter.0 {
            write!(f, "iter")?;
            Sep::Space
        } else {
            Sep::None
        };

        let multi = self.print_all(f, sep, self.kinds(&[attr.rel, attr.val]))?;

        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<(PropPattern<'a, L>, Nodes)> for Printer<'h, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        arm: &(PropPattern<'a, L>, Nodes),
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        let (pattern, nodes) = arm;
        match pattern {
            PropPattern::Attr(rel, val) => {
                write!(f, "(")?;
                self.print(f, Sep::None, rel)?;
                self.print(f, Sep::Space, val)?;
                write!(f, ")")?;
            }
            PropPattern::Set(val, HasDefault(false)) => {
                write!(f, "(..")?;
                self.print(f, Sep::Space, val)?;
                write!(f, ")")?;
            }
            PropPattern::Set(val, HasDefault(true)) => {
                write!(f, "(..default")?;
                self.print(f, Sep::Space, val)?;
                write!(f, ")")?;
            }
            PropPattern::Absent => {
                write!(f, "()")?;
            }
        }
        let multi = self.print_all(f, Sep::Space, self.kinds(nodes))?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<Clause<EvalCondTerm>> for Printer<'h, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        sep: Sep,
        clause: &Clause<EvalCondTerm>,
    ) -> PrintResult {
        write!(f, "{sep}")?;
        match clause {
            Clause::Root(var) => {
                write!(f, "(root '{var})")?;
                Ok(Multiline(true))
            }
            Clause::IsEntity(term, def_id) => {
                write!(f, "(is-entity")?;
                self.print(f, Sep::Space, term)?;
                write!(f, " {def_id:?})")?;

                Ok(Multiline(false))
            }
            Clause::Attr(var, prop_id, (rel, val)) => {
                write!(f, "(attr '{var} {prop_id} (")?;
                let multi = self.print_all(f, Sep::None, [rel, val].into_iter())?;
                self.print_rparen(multi, f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            _ => todo!(),
        }
    }
}

impl<'h, 'a, L: Lang> Print<EvalCondTerm> for Printer<'h, 'a, L> {
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

impl<'h, 'a, L: Lang> Print<CaptureMatchArm<'a, L>> for Printer<'h, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        arm: &CaptureMatchArm<'a, L>,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}((")?;
        let multi = self.print_all(f, Sep::None, arm.capture_groups.iter())?;
        self.print_rparen(multi, f)?;
        let multi = self.print_all(f, Sep::Space, self.kinds(&arm.nodes))?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<Binding<'a, L>> for Printer<'h, 'a, L> {
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

impl<'h, 'a, L: Lang> Print<CaptureGroup<'a, L>> for Printer<'h, 'a, L> {
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

impl<'h, 'a, L: Lang> Print<SetEntry<'a, L>> for Printer<'h, 'a, L> {
    fn print(
        self,
        f: &mut std::fmt::Formatter,
        _sep: Sep,
        SetEntry(iter_label, attr): &SetEntry<'a, L>,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        if let Some(iter_label) = iter_label {
            write!(f, ".. ({})", L::as_hir(iter_label))?;
        }
        let multi = self.print_all(f, self.indent.indent(), self.kinds(&[attr.rel, attr.val]))?;
        self.print_rparen(multi, f)?;
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
        self.print_rparen(multi, f)?;
        Ok(multi)
    }
}

impl<'h, 'a, L: Lang> Printer<'h, 'a, L> {
    fn print_rparen(self, multi: Multiline, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if multi.0 {
            let indent = self.indent.force_indent();
            write!(f, "{indent})")?;
        } else {
            write!(f, ")")?;
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

impl<'a, L: Lang> Display for Binding<'a, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wildcard => write!(f, "$_"),
            Self::Binder(binder) => write!(f, "{}", L::as_hir(binder).var),
        }
    }
}

impl Debug for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Label({})", AsAlpha(self.0))
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}", AsAlpha(self.0))
    }
}
