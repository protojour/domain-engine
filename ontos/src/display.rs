use ontol_runtime::vm::proc::BuiltinProc;

use crate::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern, PropVariant},
    Lang, Node,
};

impl<'a, L: Lang> std::fmt::Display for NodeKind<'a, L> {
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

impl<'a, L: Lang> Print<NodeKind<'a, L>> for Printer<L> {
    fn print(self, sep: Sep, kind: &NodeKind<'a, L>, f: &mut std::fmt::Formatter) -> PrintResult {
        let indent = self.indent;
        match kind {
            NodeKind::VariableRef(var) => {
                write!(f, "{sep}#{}", var.0)?;
                Ok(sep.multiline())
            }
            NodeKind::Int(int) => {
                write!(f, "{sep}{int}")?;
                Ok(sep.multiline())
            }
            NodeKind::Unit => {
                write!(f, "{sep}#u")?;
                Ok(sep.multiline())
            }
            NodeKind::Let(binder, definition, body) => {
                write!(f, "{indent}(let (#{}", binder.0 .0)?;
                let multi = self.print(Sep::Space, definition.kind(), f)?;
                self.print_rparen(multi, f)?;
                let multi = self.print_all(Sep::Space, body.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            NodeKind::Call(proc, args) => {
                let proc = match proc {
                    BuiltinProc::Add => "+",
                    BuiltinProc::Sub => "+",
                    BuiltinProc::Mul => "*",
                    BuiltinProc::Div => "/",
                    proc => panic!("unsupported proc {proc:?}"),
                };
                write!(f, "{sep}({proc}")?;
                let multi = self.print_all(Sep::Space, args.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(multi)
            }
            NodeKind::Seq(binder, children) => {
                write!(f, "{indent}(seq (#{})", binder.0 .0)?;
                let multi = self.print_all(Sep::Space, children.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            NodeKind::Struct(binder, children) => {
                write!(f, "{indent}(struct (#{})", binder.0 .0)?;
                let multi = self.print_all(Sep::Space, children.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            NodeKind::Prop(struct_var, prop, variant) => {
                write!(f, "{indent}(prop #{} {prop}", struct_var.0)?;
                let multi = self.print_all(Sep::Space, [variant].into_iter(), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            NodeKind::MapSeq(var, binder, children) => {
                write!(f, "{indent}(map-seq #{} (#{})", var.0, binder.0 .0)?;
                let multi = self.print_all(Sep::Space, children.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            NodeKind::Destruct(arg, children) => {
                write!(f, "{indent}(destruct #{}", arg.0)?;
                let multi = self.print_all(Sep::Space, children.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            NodeKind::MatchProp(struct_var, prop, arms) => {
                write!(f, "{indent}(match-prop #{} {}", struct_var.0, prop)?;
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
        node: &PropVariant<'a, L>,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        let multi = self.print_all(Sep::None, [node.rel.kind(), node.val.kind()].into_iter(), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<'a, L: Lang> Print<MatchArm<'a, L>> for Printer<L> {
    fn print(self, _sep: Sep, node: &MatchArm<'a, L>, f: &mut std::fmt::Formatter) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        match &node.pattern {
            PropPattern::Present(rel, val) => {
                write!(f, "(")?;
                self.print(Sep::None, rel, f)?;
                self.print(Sep::Space, val, f)?;
                write!(f, ")")?;
            }
            PropPattern::NotPresent => {
                write!(f, "()")?;
            }
        }
        let multi = self.print_all(Sep::Space, node.nodes.iter().map(Node::kind), f)?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl<L: Lang> Print<PatternBinding> for Printer<L> {
    fn print(self, sep: Sep, ast: &PatternBinding, f: &mut std::fmt::Formatter) -> PrintResult {
        match ast {
            PatternBinding::Binder(var) => {
                write!(f, "{sep}#{}", var.0)?;
            }
            PatternBinding::Wildcard => {
                write!(f, "{sep}#_")?;
            }
        }
        Ok(Multiline(false))
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
