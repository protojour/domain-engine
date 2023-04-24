use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use smallvec::SmallVec;
use tracing::debug;

use crate::{
    codegen::{find_mapping_key, proc_builder::Stack, value_pattern::codegen_value_pattern_origin},
    ir_node::{Body, CodeDirection, IrKind, IrNodeId, SyntaxVar},
    SourceSpan,
};

use super::{
    equation::IrNodeEquation,
    ir::{Ir, Terminator},
    proc_builder::{Block, ProcBuilder},
    struct_pattern::codegen_struct_pattern_origin,
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

pub(super) struct CodeGenerator<'t, 'b> {
    proc_table: &'t mut ProcTable,
    pub builder: &'b mut ProcBuilder,
    direction: CodeDirection,

    var_stack: SmallVec<[Box<dyn CodegenVariable>; 3]>,
}

impl<'t, 'b> CodeGenerator<'t, 'b> {
    pub fn new(
        proc_table: &'t mut ProcTable,
        builder: &'b mut ProcBuilder,
        direction: CodeDirection,
    ) -> Self {
        Self {
            proc_table,
            builder,
            direction,
            var_stack: Default::default(),
        }
    }

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

    pub fn codegen_body(&mut self, block: &mut Block, equation: &IrNodeEquation, body: &Body) {
        let (bindings_id, output_id) = body.order(self.direction);
        let (_, source_pattern, _) = equation.resolve_node(&equation.reductions, bindings_id);

        match &source_pattern.kind {
            IrKind::ValuePattern(_) => {
                let to_def = &equation.nodes[output_id].ty.get_single_def_id().unwrap();

                codegen_value_pattern_origin(self, block, equation, output_id, *to_def)
            }
            IrKind::StructPattern(attrs) => {
                codegen_struct_pattern_origin(self, block, equation, output_id, attrs)
            }
            other => panic!("unable to generate mapping for pattern: {other:?}"),
        }
    }

    // Generate a node interpreted as an expression, i.e. a computation producing one or many values.
    pub fn codegen_expr(
        &mut self,
        block: &mut Block,
        equation: &IrNodeEquation,
        node_id: IrNodeId,
    ) {
        let (_, expr, span) = equation.resolve_node(&equation.expansions, node_id);
        match &expr.kind {
            IrKind::Call(proc, params) => {
                let stack_delta = Stack(-(params.len() as i32) + 1);

                for param in params.iter() {
                    self.codegen_expr(block, equation, *param);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                self.builder.push(
                    block,
                    Ir::CallBuiltin(*proc, return_def_id),
                    stack_delta,
                    span,
                );
            }
            IrKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();

                self.builder
                    .push(block, Ir::Constant(*k, return_def_id), Stack(1), span);
            }
            IrKind::Variable(var) => {
                self.codegen_variable(block, *var, &span);
            }
            IrKind::VariableRef(_) => panic!(),
            IrKind::MapCall(param_id, from_ty) => {
                self.codegen_expr(block, equation, *param_id);

                debug!(
                    "map value from {from_ty:?} to {:?}, span = {span:?}",
                    expr.ty
                );
                let from = find_mapping_key(from_ty).unwrap();
                let to = find_mapping_key(&expr.ty).unwrap();

                let proc = Procedure {
                    address: self.proc_table.gen_mapping_addr(from, to),
                    n_params: NParams(1),
                };

                self.builder.push(block, Ir::Call(proc), Stack(0), span);
            }
            IrKind::Aggr(_) => todo!(),
            IrKind::MapSequence(seq_id, iter_var, body_id, _) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                let output_seq = self.builder.push(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewSeq, return_def_id),
                    Stack(1),
                    span,
                );

                // Input sequence:
                self.codegen_expr(block, equation, *seq_id);
                let input_seq = self.builder.top();

                let counter =
                    self.builder
                        .push(block, Ir::Constant(0, DefId::unit()), Stack(1), span);

                let for_each_offset = block.current_offset();

                let rel_params_local = self.builder.top_plus(1);
                let value_local = self.builder.top_plus(2);
                let codegen_iter = CodegenIter {
                    iter_var: *iter_var,
                    rel_params_local,
                    value_local,
                };

                let for_each_body_index = self.enter_bind_level(codegen_iter, |gen| {
                    // inside the for-each body there are two items on the stack, value (top), then rel_params
                    let mut block2 = gen.builder.new_block(Stack(2), span);

                    gen.codegen_expr(&mut block2, equation, *body_id);
                    gen.builder
                        .push(&mut block2, Ir::Clone(rel_params_local), Stack(1), span);
                    // still two items on the stack: append to original sequence
                    // for now, rel_params is not mapped
                    gen.builder
                        .push(&mut block2, Ir::AppendAttr2(output_seq), Stack(-2), span);
                    gen.builder
                        .push(&mut block2, Ir::Remove(value_local), Stack(-1), span);
                    gen.builder
                        .push(&mut block2, Ir::Remove(rel_params_local), Stack(-1), span);

                    gen.builder
                        .commit(block2, Terminator::PopGoto(block.index(), for_each_offset))
                });

                self.builder.push(
                    block,
                    Ir::Iter(input_seq, counter, for_each_body_index),
                    Stack(0),
                    span,
                );
                self.builder
                    .push(block, Ir::Remove(counter), Stack(-1), span);
                self.builder
                    .push(block, Ir::Remove(input_seq), Stack(-1), span);
            }
            IrKind::ValuePattern(_) => {
                todo!()
            }
            IrKind::StructPattern(attrs) => {
                let def_id = &equation.nodes[node_id].ty.get_single_def_id().unwrap();
                let local = self.builder.push(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewMap, *def_id),
                    Stack(1),
                    span,
                );

                for (property_id, node_id) in attrs {
                    self.codegen_expr(block, equation, *node_id);
                    self.builder.push(
                        block,
                        Ir::PutAttrValue(local, *property_id),
                        Stack(-1),
                        span,
                    );
                }
            }
            IrKind::Unit => {
                todo!()
            }
        }
    }

    fn codegen_variable(&mut self, block: &mut Block, var: SyntaxVar, span: &SourceSpan) {
        let bind_depth = var.1;
        debug!(
            "bind_depth: {bind_depth:?} var_stack len: {}",
            self.var_stack.len()
        );
        let generator = &mut self.var_stack[bind_depth.0 as usize];
        generator.codegen_variable(self.builder, block, var, span);
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
        builder.push(block, Ir::Clone(self.value_local), Stack(1), *span);
    }
}
