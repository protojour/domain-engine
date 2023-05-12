use ontol_runtime::vm::proc::BuiltinProc;

use super::ast::{Hir2Ast, Hir2AstPatternBinding, Hir2AstPropMatchArm, Hir2AstPropPattern};

impl std::fmt::Display for Hir2Ast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Printer::new().print(Sep::None, self, f)?;
        Ok(())
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
struct Printer {
    indent: Indent,
}

impl Print<Hir2Ast> for Printer {
    fn print(self, sep: Sep, ast: &Hir2Ast, f: &mut std::fmt::Formatter) -> PrintResult {
        let indent = self.indent;
        match ast {
            Hir2Ast::VariableRef(var) => {
                write!(f, "{sep}#{}", var.0)?;
                Ok(sep.multiline())
            }
            Hir2Ast::Int(int) => {
                write!(f, "{sep}{int}")?;
                Ok(sep.multiline())
            }
            Hir2Ast::Unit => {
                write!(f, "{sep}#u")?;
                Ok(sep.multiline())
            }
            Hir2Ast::Call(proc, args) => {
                let proc = match proc {
                    BuiltinProc::Add => "+",
                    BuiltinProc::Sub => "+",
                    BuiltinProc::Mul => "*",
                    BuiltinProc::Div => "/",
                    proc => panic!("unsupported proc {proc:?}"),
                };
                write!(f, "({proc}")?;
                let multi = self.print_all(Sep::Space, args, f)?;
                self.print_rparen(multi, f)?;
                Ok(multi)
            }
            Hir2Ast::Construct(children) => {
                write!(f, "{indent}(construct")?;
                let multi = self.print_all(Sep::Space, children, f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Hir2Ast::ConstructProp(prop, rel, val) => {
                write!(f, "{indent}(construct-prop {prop}")?;
                let multi = self.print_all(Sep::Space, [rel.as_ref(), val.as_ref()], f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Hir2Ast::Destruct(arg, children) => {
                write!(f, "{indent}(destruct #{}", arg.0)?;
                let multi = self.print_all(Sep::Space, children, f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
            Hir2Ast::DestructProp(prop, arms) => {
                write!(f, "{indent}(destruct-prop {}", prop)?;
                let multi = self.print_all(Sep::Space, arms, f)?;
                self.print_rparen(multi, f)?;
                Ok(Multiline(true))
            }
        }
    }
}

impl Print<Hir2AstPropMatchArm> for Printer {
    fn print(
        self,
        _sep: Sep,
        ast: &Hir2AstPropMatchArm,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        let indent = self.indent;
        write!(f, "{indent}(")?;
        match &ast.pattern {
            Hir2AstPropPattern::Present(rel, val) => {
                write!(f, "(")?;
                self.print(Sep::None, rel, f)?;
                self.print(Sep::Space, val, f)?;
                write!(f, ")")?;
            }
            Hir2AstPropPattern::NotPresent => {
                write!(f, "()")?;
            }
        }
        let multi = self.indented(|p| p.print(Sep::Space, &ast.node, f))?;
        self.print_rparen(multi, f)?;
        Ok(Multiline(true))
    }
}

impl Print<Hir2AstPatternBinding> for Printer {
    fn print(
        self,
        sep: Sep,
        ast: &Hir2AstPatternBinding,
        f: &mut std::fmt::Formatter,
    ) -> PrintResult {
        match ast {
            Hir2AstPatternBinding::Binder(var) => {
                write!(f, "{sep}#{}", var.0)?;
            }
            Hir2AstPatternBinding::Wildcard => {
                write!(f, "{sep}#_")?;
            }
        }
        Ok(Multiline(false))
    }
}

impl Printer {
    pub fn new() -> Self {
        Self { indent: Indent(0) }
    }

    fn print_rparen(self, multi: Multiline, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if multi.0 {
            let indent = self.indent;
            write!(f, "{indent})")?;
        } else {
            write!(f, ")")?;
        }
        Ok(())
    }

    fn indented(self, mut func: impl FnMut(Printer) -> PrintResult) -> PrintResult {
        let mut indented = self;
        indented.indent.0 += 1;
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
        I: IntoIterator<Item = &'a T>,
    {
        let mut printer = self;
        printer.indent.0 += 1;
        let mut multiline = Multiline(false);
        for ast in asts {
            multiline = printer.print(sep, ast, f)?;
            if multiline.0 {
                sep = Sep::Indent(printer.indent.0);
            } else {
                sep = Sep::Space;
            }
        }
        Ok(multiline)
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
