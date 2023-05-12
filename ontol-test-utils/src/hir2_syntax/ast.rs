use ontol_runtime::vm::proc::BuiltinProc;

#[derive(Debug)]
pub enum Hir2Ast {
    VariableRef(Hir2AstVariable),
    Int(i64),
    Unit,
    Call(BuiltinProc, Vec<Hir2Ast>),
    Construct(Vec<Hir2Ast>),
    ConstructProp(String, Box<Hir2Ast>, Box<Hir2Ast>),
    Destruct(Hir2AstVariable, Vec<Hir2Ast>),
    DestructProp(String, Vec<Hir2AstPropMatchArm>),
}

#[derive(Debug)]
pub struct Hir2AstVariable(pub u32);

#[derive(Debug)]
pub struct Hir2AstPropMatchArm {
    pub pattern: Hir2AstPropPattern,
    pub node: Hir2Ast,
}

#[derive(Debug)]
pub enum Hir2AstPropPattern {
    Present(Hir2AstPatternBinding, Hir2AstPatternBinding),
    NotPresent,
}

#[derive(Debug)]
pub enum Hir2AstPatternBinding {
    Wildcard,
    Binder(u32),
}
