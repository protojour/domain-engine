// TODO: Use miette
pub enum CompileError {
    WrongNumberOfArguments,
    NotAFunction,
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
