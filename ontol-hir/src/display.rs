use std::fmt::{Debug, Display};

use ontol_runtime::vm::proc::BuiltinProc;

use crate::{
    arena::{Arena, NodeRef},
    Attribute, Binding, CaptureGroup, CaptureMatchArm, HasDefault, Iter, Kind, Label, Lang, Node,
    Nodes, PropPattern, PropVariant, RootNode, StructFlags, Var,
};

impl<'h, 'a, L: Lang> std::fmt::Display for NodeRef<'h, 'a, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let printer = Printer {
            indent: Sep::None,
            arena: self.arena,
        };
        printer.print(Sep::None, self.arena.kind_of(self.node), f)?;
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
    fn print(self, sep: Sep, node: &T, f: &mut std::fmt::Formatter) -> PrintResult;
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
    fn print(self, sep: Sep, kind: &Kind<'a, L>, f: &mut std::fmt::Formatter) -> PrintResult {
        let indent = self.indent;
        match kind {
            Kind::Var(var) => {
                write!(f, "{sep}{}", var)?;
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
            Kind::Let(binder, definition, body) => {
                write!(f, "{indent}(let ({}", L::as_hir(binder).var)?;
                let multi = self.print(Sep::Space, self.kind(*definition), f)?;
                self.print_rparen(multi, f)?;
                let multi = self.print_all(self.indent.indent(), self.kinds(body), f)?;
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
                let multi = self.print_all(Sep::Space, self.kinds(args), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi.or(sep))
            }
            Kind::Map(arg) => {
                write!(f, "{sep}(map")?;
                let multi = self.print_all(Sep::Space, [self.kind(*arg)].into_iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi.or(sep))
            }
            Kind::DeclSeq(label, attr) => {
                write!(f, "{indent}(decl-seq ({})", L::as_hir(label))?;
                let multi = self.print_all(Sep::Space, self.kinds(&[attr.rel, attr.val]), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Struct(binder, flags, children) => {
                if flags.contains(StructFlags::MATCH) {
                    write!(f, "{indent}(match-struct ({})", L::as_hir(binder).var)?;
                } else {
                    write!(f, "{indent}(struct ({})", L::as_hir(binder).var)?;
                }
                let multi = self.print_all(Sep::Space, self.kinds(children), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Prop(optional, struct_var, prop_id, variants) => {
                write!(
                    f,
                    "{indent}(prop{} {struct_var} {prop_id}",
                    if optional.0 { "?" } else { "" }
                )?;
                let multi = self.print_all(Sep::Space, variants.iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::MatchProp(struct_var, id, arms) => {
                write!(f, "{indent}(match-prop {struct_var} {id}")?;
                let multi = self.print_all(Sep::Space, arms.iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Sequence(binder, children) => {
                let indent = if children.is_empty() { sep } else { indent };
                write!(f, "{indent}(sequence ({})", L::as_hir(binder).var)?;
                let multi = self.print_all(Sep::Space, self.kinds(children), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi)
            }
            Kind::ForEach(var, (rel, val), children) => {
                write!(f, "{indent}(for-each {var} ({rel} {val})")?;
                let multi = self.print_all(Sep::Space, self.kinds(children), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::SeqPush(seq_var, attr) => {
                write!(f, "{indent}(seq-push {seq_var}",)?;
                let multi = self.print_all(Sep::Space, self.kinds(&[attr.rel, attr.val]), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::StringPush(to_var, node) => {
                write!(f, "{indent}(string-push {to_var}",)?;
                let multi = self.print(Sep::Space, self.kind(*node), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Regex(label, regex_def_id, captures) => {
                if let Some(label) = label {
                    write!(f, "{sep}(regex-seq ({}) {regex_def_id:?}", L::as_hir(label))?;
                } else {
                    write!(f, "{sep}(regex {regex_def_id:?}")?;
                }
                let multi = self.print_all(Sep::Space, captures.iter(), f)?;
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
                let multi = self.print_all(Sep::Space, arms.iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<'h, 'a, L: Lang> Print<PropVariant<'a, L>> for Printer<'h, 'a, L> {
    fn print(
        self,
        _sep: Sep,
        variant: &PropVariant<'a, L>,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;

        let multi = match variant {
            PropVariant::Singleton(attr) => {
                self.print_all(Sep::None, self.kinds(&[attr.rel, attr.val]), f)?
            }
            PropVariant::Seq(seq_variant) => {
                if seq_variant.has_default.0 {
                    write!(f, "seq-default")?;
                } else {
                    write!(f, "seq")?;
                }
                write!(f, " ({})", L::as_hir(&seq_variant.label))?;

                self.print_all(Sep::Space, seq_variant.elements.iter(), f)?;
                Multiline(true)
            }
        };

        // let multi = self.print_all(sep, [attr.rel.kind(), attr.val.kind()].into_iter(), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<(Iter, Attribute<Node>)> for Printer<'h, 'a, L> {
    fn print(
        self,
        _sep: Sep,
        (iter, attr): &(Iter, Attribute<Node>),
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;

        let sep = if iter.0 {
            write!(f, "iter")?;
            Sep::Space
        } else {
            Sep::None
        };

        let multi = self.print_all(sep, self.kinds(&[attr.rel, attr.val]), f)?;

        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<(PropPattern<'a, L>, Nodes)> for Printer<'h, 'a, L> {
    fn print(
        self,
        _sep: Sep,
        arm: &(PropPattern<'a, L>, Nodes),
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        let (pattern, nodes) = arm;
        match pattern {
            PropPattern::Attr(rel, val) => {
                write!(f, "(")?;
                self.print(Sep::None, rel, f)?;
                self.print(Sep::Space, val, f)?;
                write!(f, ")")?;
            }
            PropPattern::Seq(val, HasDefault(false)) => {
                write!(f, "(seq")?;
                self.print(Sep::Space, val, f)?;
                write!(f, ")")?;
            }
            PropPattern::Seq(val, HasDefault(true)) => {
                write!(f, "(seq-default")?;
                self.print(Sep::Space, val, f)?;
                write!(f, ")")?;
            }
            PropPattern::Absent => {
                write!(f, "()")?;
            }
        }
        let multi = self.print_all(Sep::Space, self.kinds(nodes), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<CaptureMatchArm<'a, L>> for Printer<'h, 'a, L> {
    fn print(
        self,
        _sep: Sep,
        arm: &CaptureMatchArm<'a, L>,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}((")?;
        let multi = self.print_all(Sep::None, arm.capture_groups.iter(), f)?;
        self.print_rparen(multi, f)?;
        let multi = self.print_all(Sep::Space, self.kinds(&arm.nodes), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'h, 'a, L: Lang> Print<Binding<'a, L>> for Printer<'h, 'a, L> {
    fn print(self, sep: Sep, ast: &Binding<'a, L>, f: &mut std::fmt::Formatter) -> PrintResult {
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
        sep: Sep,
        group: &CaptureGroup<'a, L>,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        write!(f, "{sep}({} {})", group.index, L::as_hir(&group.binder).var)?;
        Ok(Multiline(false))
    }
}

impl<'h, 'a, L: Lang, T> Print<Vec<T>> for Printer<'h, 'a, L>
where
    Printer<'h, 'a, L>: Print<T>,
{
    fn print(self, sep: Sep, node: &Vec<T>, f: &mut std::fmt::Formatter) -> PrintResult {
        write!(f, "{sep}(")?;
        let multi = self.print_all(Sep::None, node.iter(), f)?;
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
        mut sep: Sep,
        asts: I,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult
    where
        Self: Print<T>,
        I: Iterator<Item = &'b T>,
    {
        let mut printer = self;
        printer.indent = self.indent.indent();
        let mut multiline = Multiline(false);
        for ast in asts {
            let last_multiline = printer.print(sep, ast, f)?;
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

pub struct AsAlpha(pub u32);

impl Display for AsAlpha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 >= 26 {
            write!(f, "{}", AsAlpha((self.0 / 26) - 1))?;
        }

        let rem = self.0 % 26;
        write!(f, "{}", char::from_u32(u32::from('a') + rem).unwrap())
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

impl Debug for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Var({})", AsAlpha(self.0))
    }
}

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", AsAlpha(self.0))
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

#[test]
fn test_as_alpha() {
    assert_eq!("a", format!("{}", AsAlpha(0)));
    assert_eq!("z", format!("{}", AsAlpha(25)));
    assert_eq!("aa", format!("{}", AsAlpha(26)));
    assert_eq!("az", format!("{}", AsAlpha(51)));
    assert_eq!("ba", format!("{}", AsAlpha(52)));
    assert_eq!("yq", format!("{}", AsAlpha(666)));
}
