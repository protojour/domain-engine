use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use smallvec::SmallVec;
use tracing::{debug, error};

use crate::{
    codegen::{find_mapping_key, proc_builder::Stack, value_pattern::codegen_value_pattern_origin},
    hir_node::{CodeDirection, HirBody, HirIdx, HirKind, HirVariable},
    types::Type,
    SourceSpan,
};

use super::{
    equation::HirEquation,
    ir::{Ir, Terminator},
    proc_builder::{Block, ProcBuilder},
    struct_pattern::codegen_struct_pattern_origin,
    ProcTable,
};

#[derive(Default)]
pub struct Scope {
    pub in_scope: FnvHashMap<HirVariable, Local>,
}

pub(super) struct CodeGenerator<'t, 'b> {
    proc_table: &'t mut ProcTable,
    pub builder: &'b mut ProcBuilder,
    direction: CodeDirection,

    scope_stack: SmallVec<[Scope; 3]>,
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
            scope_stack: Default::default(),
        }
    }

    pub fn enter_scope<T>(&mut self, scope: Scope, f: impl FnOnce(&mut Self) -> T) -> T {
        debug!("enter scope: {:?}", scope.in_scope);
        self.scope_stack.push(scope);
        let result = f(self);
        self.scope_stack.pop();
        result
    }

    pub fn codegen_body(&mut self, block: &mut Block, equation: &HirEquation, body: &HirBody) {
        let (bindings_id, output_id) = body.order(self.direction);
        let (_, source_pattern, _) = equation.resolve_node(&equation.reductions, bindings_id);

        match &source_pattern.kind {
            HirKind::ValuePattern(_) => {
                let to_def = &equation.nodes[output_id].ty.get_single_def_id().unwrap();

                codegen_value_pattern_origin(self, block, equation, output_id, *to_def)
            }
            HirKind::StructPattern(attrs) => {
                codegen_struct_pattern_origin(self, block, equation, output_id, attrs)
            }
            other => panic!("unable to generate mapping for pattern: {other:?}"),
        }
    }

    // Generate a node interpreted as an expression, i.e. a computation producing one or many values.
    pub fn codegen_expr(&mut self, block: &mut Block, equation: &HirEquation, node_idx: HirIdx) {
        let (_, expr, span) = equation.resolve_node(&equation.expansions, node_idx);
        match &expr.kind {
            HirKind::Call(proc, params) => {
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
            HirKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();

                self.builder
                    .push(block, Ir::Constant(*k, return_def_id), Stack(1), span);
            }
            HirKind::Variable(var) => {
                self.codegen_variable(block, *var, &span);
            }
            HirKind::VariableRef(_) => panic!(),
            HirKind::MapCall(param_id, from_ty) => {
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
            HirKind::Aggr(seq_idx, body_idx) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                let output = match expr.ty {
                    Type::Array(_) => self.builder.push(
                        block,
                        Ir::CallBuiltin(BuiltinProc::NewSeq, return_def_id),
                        Stack(1),
                        span,
                    ),
                    _ => todo!("Aggregate to other types than array"),
                };

                // Input sequence:
                self.codegen_expr(block, equation, *seq_idx);

                let counter =
                    self.builder
                        .push(block, Ir::Constant(0, DefId::unit()), Stack(1), span);

                let iter_offset = block.current_offset();

                let rel_params_local = self.builder.top_plus(1);
                let value_local = self.builder.top_plus(2);

                /*
                let codegen_iter = CodegenIter {
                    iter_var: *iter_var,
                    rel_params_local,
                    value_local,
                };
                */
            }
            HirKind::MapSequence(seq_idx, iter_var, body_idx, _) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                let output_seq = self.builder.push(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewSeq, return_def_id),
                    Stack(1),
                    span,
                );

                // Input sequence:
                self.codegen_expr(block, equation, *seq_idx);
                let input_seq = self.builder.top();

                let counter =
                    self.builder
                        .push(block, Ir::Constant(0, DefId::unit()), Stack(1), span);

                let iter_offset = block.current_offset();

                let rel_params_local = self.builder.top_plus(1);
                let value_local = self.builder.top_plus(2);

                let mut scope = Scope::default();
                scope.in_scope.insert(*iter_var, value_local);

                let for_each_body_index = self.enter_scope(scope, |gen| {
                    // inside the for-each body there are two items on the stack, value (top), then rel_params
                    let mut block2 = gen.builder.new_block(Stack(2), span);

                    gen.codegen_expr(&mut block2, equation, *body_idx);
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
                        .commit(block2, Terminator::PopGoto(block.index(), iter_offset))
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
            HirKind::ValuePattern(_) => {
                todo!()
            }
            HirKind::StructPattern(attrs) => {
                let def_id = &equation.nodes[node_idx].ty.get_single_def_id().unwrap();
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
            HirKind::Unit => {
                todo!()
            }
        }
    }

    fn codegen_variable(&mut self, block: &mut Block, var: HirVariable, span: &SourceSpan) {
        for scope in self.scope_stack.iter().rev() {
            if let Some(local) = scope.in_scope.get(&var) {
                self.builder.push(block, Ir::Clone(*local), Stack(1), *span);
                return;
            }
        }

        for scope in &self.scope_stack {
            error!("scope: {:?}", scope.in_scope);
        }

        panic!("{var:?} not in scope!");
    }
}
