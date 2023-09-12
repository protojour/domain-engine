use std::fmt::{Debug, Display};

use ontol_runtime::vm::proc::BuiltinProc;

use crate::{
    Attribute, Binding, CaptureGroup, CaptureMatchArm, GetKind, GetLabel, GetVar, HasDefault, Iter,
    Kind, Label, Lang, PropMatchArm, PropPattern, PropVariant, StructFlags, Var,
};

impl<'a, L: Lang> std::fmt::Display for Kind<'a, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Printer::default().print(Sep::None, self, f)?;
        Ok(())
    }
}

type PrintResult = Result<Multiline, std::fmt::Error>;

pub trait Print<T>: Copy {
    fn print(self, sep: Sep, node: &T, f: &mut std::fmt::Formatter) -> PrintResult;
}

#[derive(Clone, Copy)]
pub struct Printer<L: Lang> {
    indent: Sep,
    lang: std::marker::PhantomData<L>,
}

impl<'a, L: Lang> Print<Kind<'a, L>> for Printer<L> {
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
            Kind::String(string) => {
                write!(f, "{sep}'{string}'")?;
                Ok(sep.multiline())
            }
            Kind::Const(const_def_id) => {
                write!(f, "{sep}(const {const_def_id:?})")?;
                Ok(sep.multiline())
            }
            Kind::Let(binder, definition, body) => {
                write!(f, "{indent}(let ({}", binder.var())?;
                let multi = self.print(Sep::Space, definition.kind(), f)?;
                self.print_rparen(multi, f)?;
                let multi =
                    self.print_all(self.indent.indent(), body.iter().map(GetKind::kind), f)?;
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
                let multi = self.print_all(Sep::Space, args.iter().map(GetKind::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi.or(sep))
            }
            Kind::Map(arg) => {
                write!(f, "{sep}(map")?;
                let multi =
                    self.print_all(Sep::Space, [arg.as_ref()].into_iter().map(GetKind::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi.or(sep))
            }
            Kind::DeclSeq(label, attr) => {
                write!(f, "{indent}(decl-seq ({})", label.label())?;
                let multi = self.print_all(
                    Sep::Space,
                    [attr.rel.as_ref(), attr.val.as_ref()]
                        .into_iter()
                        .map(GetKind::kind),
                    f,
                )?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Struct(binder, flags, children) => {
                if flags.contains(StructFlags::MATCH) {
                    write!(f, "{indent}(match-struct ({})", binder.var())?;
                } else {
                    write!(f, "{indent}(struct ({})", binder.var())?;
                }
                let multi = self.print_all(Sep::Space, children.iter().map(GetKind::kind), f)?;
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
                write!(f, "{indent}(sequence ({})", binder.var())?;
                let multi = self.print_all(Sep::Space, children.iter().map(GetKind::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi)
            }
            Kind::ForEach(var, (rel, val), children) => {
                write!(f, "{indent}(for-each {var} ({rel} {val})")?;
                let multi = self.print_all(Sep::Space, children.iter().map(GetKind::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::SeqPush(seq_var, attr) => {
                write!(f, "{indent}(seq-push {seq_var}",)?;
                let multi = self.print_all(
                    Sep::Space,
                    [attr.rel.as_ref(), attr.val.as_ref()]
                        .into_iter()
                        .map(GetKind::kind),
                    f,
                )?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::StringPush(to_var, node) => {
                write!(f, "{indent}(string-push {to_var}",)?;
                let multi = self.print(Sep::Space, node.kind(), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Kind::Regex(label, regex_def_id, captures) => {
                if let Some(label) = label {
                    write!(f, "{sep}(regex-seq ({}) {regex_def_id:?}", label.label())?;
                } else {
                    write!(f, "{sep}(regex {regex_def_id:?}")?;
                }
                let multi = self.print_all(Sep::Space, captures.iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(sep.multiline())
            }
            Kind::MatchRegex(var, regex_def_id, arms) => {
                write!(f, "{indent}(match-regex {var} {regex_def_id:?}")?;
                let multi = self.print_all(Sep::Space, arms.iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<'a, L: Lang> Print<PropVariant<'a, L>> for Printer<L> {
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
                self.print_all(Sep::None, [attr.rel.kind(), attr.val.kind()].into_iter(), f)?
            }
            PropVariant::Seq(seq_variant) => {
                if seq_variant.has_default.0 {
                    write!(f, "seq-default")?;
                } else {
                    write!(f, "seq")?;
                }
                write!(f, " ({})", seq_variant.label.label())?;

                self.print_all(Sep::Space, seq_variant.elements.iter(), f)?;
                Multiline(true)
            }
        };

        // let multi = self.print_all(sep, [attr.rel.kind(), attr.val.kind()].into_iter(), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'a, L: Lang> Print<(Iter, Attribute<L::Node<'a>>)> for Printer<L> {
    fn print(
        self,
        _sep: Sep,
        (iter, attr): &(Iter, Attribute<L::Node<'a>>),
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

        let multi = self.print_all(sep, [attr.rel.kind(), attr.val.kind()].into_iter(), f)?;

        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'a, L: Lang> Print<PropMatchArm<'a, L>> for Printer<L> {
    fn print(
        self,
        _sep: Sep,
        arm: &PropMatchArm<'a, L>,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        match &arm.pattern {
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
        let multi = self.print_all(Sep::Space, arm.nodes.iter().map(GetKind::kind), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'a, L: Lang> Print<CaptureMatchArm<'a, L>> for Printer<L> {
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
        let multi = self.print_all(Sep::Space, arm.nodes.iter().map(GetKind::kind), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'a, L: Lang> Print<Binding<'a, L>> for Printer<L> {
    fn print(self, sep: Sep, ast: &Binding<'a, L>, f: &mut std::fmt::Formatter) -> PrintResult {
        match ast {
            Binding::Binder(binder) => {
                write!(f, "{sep}{}", binder.var())?;
            }
            Binding::Wildcard => {
                write!(f, "{sep}$_")?;
            }
        }
        Ok(Multiline(false))
    }
}

impl<'a, L: Lang> Print<CaptureGroup<'a, L>> for Printer<L> {
    fn print(
        self,
        sep: Sep,
        group: &CaptureGroup<'a, L>,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        write!(f, "{sep}({} {})", group.index, group.binder.var())?;
        Ok(Multiline(false))
    }
}

impl<L: Lang, T> Print<Vec<T>> for Printer<L>
where
    Printer<L>: Print<T>,
{
    fn print(self, sep: Sep, node: &Vec<T>, f: &mut std::fmt::Formatter) -> PrintResult {
        write!(f, "{sep}(")?;
        let multi = self.print_all(Sep::None, node.iter(), f)?;
        self.print_rparen(multi, f)?;
        Ok(multi)
    }
}

impl<L: Lang> Default for Printer<L> {
    fn default() -> Self {
        Self {
            indent: Sep::None,
            lang: std::marker::PhantomData,
        }
    }
}

impl<L: Lang> Printer<L> {
    fn print_rparen(self, multi: Multiline, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if multi.0 {
            let indent = self.indent.force_indent();
            write!(f, "{indent})")?;
        } else {
            write!(f, ")")?;
        }
        Ok(())
    }

    fn print_all<'a, T: 'a, I>(
        self,
        mut sep: Sep,
        asts: I,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult
    where
        Self: Print<T>,
        I: Iterator<Item = &'a T>,
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
            Self::Binder(binder) => write!(f, "{}", binder.var()),
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
