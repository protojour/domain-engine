use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use smallvec::SmallVec;
use tracing::debug;

use crate::{
    codegen::{find_translation_key, proc_builder::Stack},
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{
    equation::TypedExprEquation,
    ir::{Ir, Terminator},
    proc_builder::{Block, ProcBuilder},
    ProcTable,
};

pub(super) trait CodegenVariable: 'static {
    fn codegen_variable(
        &mut self,
        builder: &mut ProcBuilder,
        block: &mut Block,
        var: SyntaxVar,
        span: &SourceSpan,
    );
}

#[derive(Default)]
pub(super) struct CodeGenerator {
    var_stack: SmallVec<[Box<dyn CodegenVariable>; 3]>,
}

impl CodeGenerator {
    pub fn enter_bind_level<T>(
        &mut self,
        bind_level_codegen: impl CodegenVariable,
        f: impl FnOnce(&mut Self) -> T,
    ) -> T {
        self.var_stack.push(Box::new(bind_level_codegen));
        let result = f(self);
        self.var_stack.pop();
        result
    }

    pub fn codegen_expr(
        &mut self,
        proc_table: &mut ProcTable,
        builder: &mut ProcBuilder,
        block: &mut Block,
        equation: &TypedExprEquation,
        expr_id: ExprRef,
    ) {
        let (_, expr, span) = equation.resolve_expr(&equation.expansions, expr_id);
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                let stack_delta = Stack(-(params.len() as i32) + 1);

                for param in params.iter() {
                    self.codegen_expr(proc_table, builder, block, equation, *param);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                builder.gen(
                    block,
                    Ir::CallBuiltin(*proc, return_def_id),
                    stack_delta,
                    span,
                );
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();

                builder.gen(block, Ir::Constant(*k, return_def_id), Stack(1), span);
            }
            TypedExprKind::Variable(var) => {
                self.codegen_variable(builder, block, *var, &span);
            }
            TypedExprKind::VariableRef(_) => panic!(),
            TypedExprKind::Translate(param_id, from_ty) => {
                self.codegen_expr(proc_table, builder, block, equation, *param_id);

                debug!(
                    "translate from {from_ty:?} to {:?}, span = {span:?}",
                    expr.ty
                );
                let from = find_translation_key(from_ty).unwrap();
                let to = find_translation_key(&expr.ty).unwrap();

                let proc = Procedure {
                    address: proc_table.gen_translate_addr(from, to),
                    n_params: NParams(1),
                };

                builder.gen(block, Ir::Call(proc), Stack(0), span);
            }
            TypedExprKind::SequenceMap(expr_ref, iter_var, body, _) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                let output_seq = builder.gen(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewSeq, return_def_id),
                    Stack(1),
                    span,
                );

                // Input sequence:
                self.codegen_expr(proc_table, builder, block, equation, *expr_ref);
                let input_seq = builder.top();

                let counter = builder.gen(block, Ir::Constant(0, DefId::unit()), Stack(1), span);

                let for_each_offset = block.current_offset();

                let rel_params_local = builder.top_plus(1);
                let value_local = builder.top_plus(2);
                let codegen_iter = CodegenIter {
                    iter_var: *iter_var,
                    rel_params_local,
                    value_local,
                };

                let for_each_body_index = self.enter_bind_level(codegen_iter, |zelf| {
                    // inside the for-each body there are two items on the stack, value (top), then rel_params
                    let mut block2 = builder.new_block(Stack(2), span);

                    zelf.codegen_expr(proc_table, builder, &mut block2, equation, *body);
                    builder.gen(&mut block2, Ir::Clone(rel_params_local), Stack(1), span);
                    // still two items on the stack: append to original sequence
                    // for now, rel_params are untranslated
                    builder.gen(&mut block2, Ir::AppendAttr2(output_seq), Stack(-2), span);
                    builder.gen(&mut block2, Ir::Remove(value_local), Stack(-1), span);
                    builder.gen(&mut block2, Ir::Remove(rel_params_local), Stack(-1), span);

                    builder.commit(block2, Terminator::PopGoto(block.index(), for_each_offset))
                });

                builder.gen(
                    block,
                    Ir::Iter(input_seq, counter, for_each_body_index),
                    Stack(0),
                    span,
                );
                builder.gen(block, Ir::Remove(counter), Stack(-1), span);
                builder.gen(block, Ir::Remove(input_seq), Stack(-1), span);
            }
            TypedExprKind::ValueObjPattern(_) => {
                todo!()
            }
            TypedExprKind::MapObjPattern(_) => {
                todo!()
            }
            TypedExprKind::Unit => {
                todo!()
            }
        }
    }
}

impl CodegenVariable for CodeGenerator {
    fn codegen_variable(
        &mut self,
        builder: &mut ProcBuilder,
        block: &mut Block,
        var: SyntaxVar,
        span: &SourceSpan,
    ) {
        let bind_depth = var.1;
        let generator = &mut self.var_stack[bind_depth.0 as usize];
        generator.codegen_variable(builder, block, var, span);
    }
}

#[allow(unused)]
struct CodegenIter {
    iter_var: SyntaxVar,
    rel_params_local: Local,
    value_local: Local,
}

impl CodegenVariable for CodegenIter {
    fn codegen_variable(
        &mut self,
        builder: &mut ProcBuilder,
        block: &mut Block,
        var: SyntaxVar,
        span: &SourceSpan,
    ) {
        assert!(var == self.iter_var);
        builder.gen(block, Ir::Clone(self.value_local), Stack(1), *span);
    }
}
