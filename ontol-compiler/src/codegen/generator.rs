use fnv::FnvHashMap;
use ontol_runtime::{
    vm::proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use smallvec::SmallVec;
use tracing::{debug, error};

use crate::{
    codegen::{find_mapping_key, proc_builder::Stack},
    hir_node::{CodeDirection, HirBody, HirBodyIdx, HirIdx, HirKind, HirNode, HirVariable},
    types::Type,
    SourceSpan,
};

use super::{
    equation::HirEquation,
    ir::{Ir, Terminator},
    proc_builder::{Block, ProcBuilder},
    struct_scope::codegen_struct_pattern_scope,
    ProcTable,
};

#[derive(Debug)]
pub enum CodegenError {
    InvalidScoping(HirVariable),
}

pub type CodegenResult<T> = Result<T, CodegenError>;

#[derive(Default)]
pub struct Scope {
    pub in_scope: FnvHashMap<HirVariable, Local>,
}

pub(super) struct CodeGenerator<'a> {
    proc_table: &'a mut ProcTable,
    pub builder: &'a mut ProcBuilder,
    bodies: &'a [HirBody],
    direction: CodeDirection,

    scope_stack: SmallVec<[Scope; 3]>,
}

impl<'a> CodeGenerator<'a> {
    pub fn new(
        proc_table: &'a mut ProcTable,
        builder: &'a mut ProcBuilder,
        bodies: &'a [HirBody],
        direction: CodeDirection,
    ) -> Self {
        Self {
            proc_table,
            builder,
            bodies,
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

    pub fn codegen_body(
        &mut self,
        block: &mut Block,
        equation: &HirEquation,
        body_idx: HirBodyIdx,
    ) -> CodegenResult<()> {
        let body = &self.bodies[body_idx.0 as usize];
        let (pattern_idx, expr_idx) = body.order(self.direction);
        let (_, pattern, _) = equation.resolve_node(&equation.reductions, pattern_idx);

        let scope = self.codegen_scope(block, equation, pattern);
        self.enter_scope(scope, |gen| gen.codegen_expr(block, equation, expr_idx))
    }

    fn codegen_scope(
        &mut self,
        block: &mut Block,
        equation: &HirEquation,
        pattern: &HirNode,
    ) -> Scope {
        match &pattern.kind {
            HirKind::StructPattern(attrs) => codegen_struct_pattern_scope(
                self,
                block,
                equation,
                self.builder.top(),
                attrs,
                pattern.span,
            ),
            HirKind::ValuePattern(_) => {
                let mut scope = Scope::default();
                scope
                    .in_scope
                    // FIXME, this won't always be variable 0
                    .insert(HirVariable(0), self.builder.top());
                scope
            }
            HirKind::Variable(var) => {
                let mut scope = Scope::default();
                scope.in_scope.insert(*var, self.builder.top());
                scope
            }
            other => panic!("unable to generate scope for pattern: {other:?}"),
        }
    }

    // Generate a node interpreted as an expression, i.e. a computation producing one or many values.
    pub fn codegen_expr(
        &mut self,
        block: &mut Block,
        equation: &HirEquation,
        node_idx: HirIdx,
    ) -> CodegenResult<()> {
        let (_, expr, span) = equation.resolve_node(&equation.expansions, node_idx);
        match &expr.kind {
            HirKind::Call(proc, params) => {
                let stack_delta = Stack(-(params.len() as i32) + 1);

                for param in params.iter() {
                    self.codegen_expr(block, equation, *param)?;
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                self.builder.append(
                    block,
                    Ir::CallBuiltin(*proc, return_def_id),
                    stack_delta,
                    span,
                );
            }
            HirKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();

                self.builder
                    .append(block, Ir::Constant(*k, return_def_id), Stack(1), span);
            }
            HirKind::Variable(var) => {
                self.codegen_variable(block, *var, &span)?;
            }
            HirKind::VariableRef(_) => panic!(),
            HirKind::MapCall(param_id, from_ty) => {
                self.codegen_expr(block, equation, *param_id)?;

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

                self.builder.append(block, Ir::Call(proc), Stack(0), span);
            }
            HirKind::Aggr(seq_idx, body_idx) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap_or_else(|| {
                    panic!("No def id found in {:?}", expr.ty);
                });
                let output = match expr.ty {
                    Type::Array(_) => self.builder.append(
                        block,
                        Ir::CallBuiltin(BuiltinProc::NewSeq, return_def_id),
                        Stack(1),
                        span,
                    ),
                    _ => todo!("Aggregate to other types than array"),
                };

                // Input sequence:
                self.codegen_expr(block, equation, *seq_idx)?;

                let input_seq = self.builder.top();

                let counter =
                    self.builder
                        .append(block, Ir::Constant(0, DefId::unit()), Stack(1), span);

                let iter_offset = block.current_offset();

                let rel_params_local = self.builder.top_plus(1);

                let for_each_body_index = {
                    let mut block2 = self.builder.new_block(Stack(2), span);

                    self.codegen_body(&mut block2, equation, *body_idx)?;

                    self.builder
                        .append(&mut block2, Ir::Clone(rel_params_local), Stack(1), span);

                    // still two items on the stack: append to original sequence
                    // for now, rel_params is not mapped
                    // FIXME: This is only correct for sequence generation:
                    self.builder
                        .append(&mut block2, Ir::AppendAttr2(output), Stack(-2), span);

                    self.builder.append_pop_until(block, counter, span);

                    self.builder
                        .commit(block2, Terminator::PopGoto(block.index(), iter_offset))
                };

                self.builder.append(
                    block,
                    Ir::Iter(input_seq, counter, for_each_body_index),
                    Stack(0),
                    span,
                );

                self.builder.append_pop_until(block, output, span);
            }
            HirKind::Match(_var_idx, _table) => {
                todo!()
            }
            HirKind::ValuePattern(node_idx) => self.codegen_expr(block, equation, *node_idx)?,
            HirKind::StructPattern(attrs) => {
                let def_id = &equation.nodes[node_idx].ty.get_single_def_id().unwrap();
                let local = self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewMap, *def_id),
                    Stack(1),
                    span,
                );

                for (property_id, node_id) in attrs {
                    self.codegen_expr(block, equation, *node_id)?;
                    self.builder.append(
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

        Ok(())
    }

    fn codegen_variable(
        &mut self,
        block: &mut Block,
        var: HirVariable,
        span: &SourceSpan,
    ) -> CodegenResult<()> {
        for scope in self.scope_stack.iter().rev() {
            if let Some(local) = scope.in_scope.get(&var) {
                self.builder
                    .append(block, Ir::Clone(*local), Stack(1), *span);
                return Ok(());
            }
        }

        for scope in &self.scope_stack {
            error!("scope: {:?}", scope.in_scope);
        }

        Err(CodegenError::InvalidScoping(var))
    }
}
