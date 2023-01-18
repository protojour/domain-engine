use chumsky::prelude::Simple;

use crate::parse::tree::Tree;

// TODO: Use miette
#[derive(Debug)]
pub enum CompileError {
    Multi(Vec<CompileError>),
    Lex(Simple<char>),
    Parse(Simple<Tree>),
    WrongNumberOfArguments,
    NotCallable,
    TypeNotFound,
}

#[derive(Default)]
pub struct CompileErrors {
    errors: Vec<CompileError>,
}

impl CompileErrors {
    pub fn push(&mut self, error: CompileError) {
        self.errors.push(error);
    }
}
