use ontol_runtime::vm::proc::BuiltinProc;

use crate::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern},
    Lang, Node,
};

impl<'a, L: Lang> std::fmt::Display for NodeKind<'a, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Printer::new().print(Sep::None, self, f)?;
        Ok(())
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

type PrintResult = Result<Multiline, std::fmt::Error>;

trait Print<T>: Copy {
    fn print(self, whitespace: Sep, ast: &T, f: &mut std::fmt::Formatter) -> PrintResult;
}

#[derive(Clone, Copy)]
struct Printer<L: Lang> {
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
            NodeKind::Call(proc, args) => {
                let proc = match proc {
                    BuiltinProc::Add => "+",
                    BuiltinProc::Sub => "+",
                    BuiltinProc::Mul => "*",
                    BuiltinProc::Div => "/",
                    proc => panic!("unsupported proc {proc:?}"),
                };
                write!(f, "({proc}")?;
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
            NodeKind::Prop(struct_var, prop, rel, val) => {
                write!(f, "{indent}(prop #{} {prop}", struct_var.0)?;
                let multi = self.print_all(Sep::Space, [rel.kind(), val.kind()].into_iter(), f)?;
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
            NodeKind::MapSeq(var, binder, children) => {
                write!(f, "{indent}(map-seq #{} (#{})", var.0, binder.0 .0)?;
                let multi = self.print_all(Sep::Space, children.iter().map(Node::kind), f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
        }
    }
}

impl<'a, L: Lang> Print<MatchArm<'a, L>> for Printer<L> {
    fn print(self, _sep: Sep, ast: &MatchArm<'a, L>, f: &mut std::fmt::Formatter) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        match &ast.pattern {
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
        let multi = self.indented(|p| p.print(Sep::Space, ast.node.kind(), f))?;
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

impl<L: Lang> Printer<L> {
    pub fn new() -> Self {
        Self {
            indent: Sep::None,
            lang: std::marker::PhantomData,
        }
    }

    fn print_rparen(self, multi: Multiline, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if multi.0 {
            let indent = self.indent.force_indent();
            write!(f, "{indent})")?;
        } else {
            write!(f, ")")?;
        }
        Ok(())
    }

    fn indented(self, mut func: impl FnMut(Self) -> PrintResult) -> PrintResult {
        let mut indented = self;
        indented.indent = self.indent.indent();
        func(indented)
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
struct Multiline(bool);

#[derive(Clone, Copy)]
enum Sep {
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
