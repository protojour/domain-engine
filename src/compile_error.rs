// TODO: Use miette
pub enum CompileError {
    WrongNumberOfArguments,
    NotCallable,
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
