use fnv::FnvHashMap;
use ontol_hir::{
    kind::{NodeKind, PatternBinding, PropPattern, PropVariant},
    Variable,
};
use ontol_runtime::{
    vm::proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use tracing::debug;

use crate::{
    codegen::{ir::Terminator, proc_builder::Stack},
    error::CompileError,
    typed_hir::{HirFunc, TypedHirNode},
    types::Type,
    CompileErrors, SpannedCompileError,
};

use super::{
    ir::Ir,
    proc_builder::{Block, ProcBuilder},
    task::{find_mapping_key, ProcTable},
};

pub(super) fn const_codegen(
    proc_table: &mut ProcTable,
    expr: TypedHirNode,
    def_id: DefId,
    errors: &mut CompileErrors,
) {
    debug!("Generating code for\n{}", expr);

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Stack(0), expr.meta.span);
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors,
    };
    generator.gen_node(expr, &mut block);
    builder.commit(block, Terminator::Return(builder.top()));

    proc_table.const_procedures.insert(def_id, builder);
}

pub(super) fn map_codegen(
    proc_table: &mut ProcTable,
    func: HirFunc,
    errors: &mut CompileErrors,
) -> bool {
    debug!("Generating code for\n{}", func);

    let return_ty = func.body.meta.ty;

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Stack(1), func.body.meta.span);
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors,
    };
    generator.scope.insert(func.arg.variable, Local(0));
    generator.gen_node(func.body, &mut block);
    builder.commit(block, Terminator::Return(builder.top()));

    match (find_mapping_key(func.arg.ty), find_mapping_key(return_ty)) {
        (Some(from_def), Some(to_def)) => {
            debug!("Saving proc: {:?} -> {:?}", func.arg.ty, return_ty);

            proc_table
                .map_procedures
                .insert((from_def, to_def), builder);
            true
        }
        (from_def, to_def) => {
            panic!("Problem finding def ids: ({from_def:?}, {to_def:?})");
        }
    }
}

pub(super) struct CodeGenerator<'a> {
    proc_table: &'a mut ProcTable,
    pub builder: &'a mut ProcBuilder,
    pub errors: &'a mut CompileErrors,

    scope: FnvHashMap<Variable, Local>,
}

impl<'a> CodeGenerator<'a> {
    fn gen_node(&mut self, node: TypedHirNode, block: &mut Block) {
        let ty = node.meta.ty;
        let span = node.meta.span;
        match node.kind {
            NodeKind::VariableRef(var) => match self.scope.get(&var) {
                Some(local) => {
                    self.builder
                        .append(block, Ir::Clone(*local), Stack(1), span);
                }
                None => {
                    self.errors.push(SpannedCompileError {
                        error: CompileError::UnboundVariable,
                        span: node.meta.span,
                    });
                }
            },
            NodeKind::Unit => {
                self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                    Stack(1),
                    span,
                );
            }
            NodeKind::Int(int) => {
                self.builder.append(
                    block,
                    Ir::Constant(int, ty.get_single_def_id().unwrap()),
                    Stack(1),
                    span,
                );
            }
            NodeKind::Let(binder, definition, body) => {
                self.gen_node(*definition, block);
                self.scope.insert(binder.0, self.builder.top());
                for node in body {
                    self.gen_node(node, block);
                }
                self.scope.remove(&binder.0);
            }
            NodeKind::Call(proc, params) => {
                let stack_delta = Stack(-(params.len() as i32) + 1);
                for param in params {
                    self.gen_node(param, block);
                }
                let return_def_id = ty.get_single_def_id().unwrap();
                self.builder.append(
                    block,
                    Ir::CallBuiltin(proc, return_def_id),
                    stack_delta,
                    span,
                );
            }
            NodeKind::Map(param) => {
                let from = find_mapping_key(param.meta.ty).unwrap();
                let to = find_mapping_key(ty).unwrap();

                self.gen_node(*param, block);

                let proc = Procedure {
                    address: self.proc_table.gen_mapping_addr(from, to),
                    n_params: NParams(1),
                };

                self.builder.append(block, Ir::Call(proc), Stack(0), span);
            }
            NodeKind::Seq(_label, _attr) => {
                todo!("seq");
            }
            NodeKind::Struct(binder, nodes) => {
                let def_id = ty.get_single_def_id().unwrap();
                let local = self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewStruct, def_id),
                    Stack(1),
                    span,
                );
                self.scope.insert(binder.0, local);
                for node in nodes {
                    self.gen_node(node, block);
                    self.builder.append_pop_until(block, local, span);
                }
                self.scope.remove(&binder.0);
            }
            NodeKind::Prop(struct_var, id, variants) => {
                for variant in variants {
                    if let PropVariant::Present { dimension: _, attr } = variant {
                        // FIXME: Don't ignore relation parameters!
                        // self.generate(*variant.attr.rel, block);
                        // let rel = self.builder.top();
                        self.gen_node(*attr.val, block);

                        let struct_local = self.var_local(struct_var);

                        self.builder.append(
                            block,
                            Ir::PutAttrValue(struct_local, id),
                            Stack(-1),
                            span,
                        );

                        return;
                    }
                }
            }
            NodeKind::MatchProp(struct_var, id, arms) => {
                let struct_local = self.var_local(struct_var);

                self.builder
                    .append(block, Ir::TakeAttr2(struct_local, id), Stack(2), span);

                let val_local = self.builder.top();
                let rel_local = self.builder.top_minus(1);

                if arms.len() != 1 {
                    todo!("multiple arms");
                }

                for arm in arms {
                    match arm.pattern {
                        PropPattern::Attr(rel_binding, val_binding) => self.gen_in_scope(
                            &[(rel_local, rel_binding), (val_local, val_binding)],
                            arm.nodes.into_iter(),
                            block,
                        ),
                        PropPattern::SeqAttr(rel_binding, val_binding) => {
                            let elem_ty = match ty {
                                Type::Array(elem_ty) => elem_ty,
                                _ => panic!("Not an array"),
                            };
                            let out_seq = self.builder.append(
                                block,
                                Ir::CallBuiltin(
                                    BuiltinProc::NewSeq,
                                    elem_ty
                                        .get_single_def_id()
                                        .unwrap_or_else(|| panic!("elem_ty: {elem_ty:?}")),
                                ),
                                Stack(1),
                                span,
                            );
                            let counter = self.builder.append(
                                block,
                                Ir::Constant(0, DefId::unit()),
                                Stack(1),
                                span,
                            );
                            let iter_offset = block.current_offset();
                            let elem_rel_local = self.builder.top_plus(1);
                            let elem_val_local = self.builder.top_plus(2);

                            let iter_body_index = {
                                let mut iter_block = self.builder.new_block(Stack(2), span);

                                self.gen_in_scope(
                                    &[(elem_rel_local, rel_binding), (elem_val_local, val_binding)],
                                    arm.nodes.into_iter(),
                                    block,
                                );

                                self.builder.append(
                                    &mut iter_block,
                                    Ir::Clone(elem_rel_local),
                                    Stack(1),
                                    span,
                                );

                                // still two items on the stack: append to original sequence
                                // for now, rel_params is not mapped
                                // FIXME: This is only correct for sequence generation:
                                self.builder.append(
                                    &mut iter_block,
                                    Ir::AppendAttr2(out_seq),
                                    Stack(-2),
                                    span,
                                );

                                self.builder
                                    .append_pop_until(&mut iter_block, counter, span);

                                self.builder.commit(
                                    iter_block,
                                    Terminator::PopGoto(block.index(), iter_offset),
                                )
                            };

                            self.builder.append(
                                block,
                                Ir::Iter(val_local, counter, iter_body_index),
                                Stack(0),
                                span,
                            );

                            self.builder.append_pop_until(block, out_seq, span);
                        }
                        PropPattern::Seq(seq_binding) => self.gen_in_scope(
                            &[
                                (rel_local, PatternBinding::Wildcard),
                                (val_local, seq_binding),
                            ],
                            arm.nodes.into_iter(),
                            block,
                        ),
                        PropPattern::Absent => {
                            todo!("Arm pattern not present")
                        }
                    }
                }
            }
            NodeKind::Gen(seq_var, iter_binder, nodes) => {
                let seq_local = self.var_local(seq_var);
                let elem_ty = match ty {
                    Type::Array(elem_ty) => elem_ty,
                    _ => panic!("Not an array"),
                };
                let out_seq = self.builder.append(
                    block,
                    Ir::CallBuiltin(
                        BuiltinProc::NewSeq,
                        elem_ty
                            .get_single_def_id()
                            .unwrap_or_else(|| panic!("elem_ty: {elem_ty:?}")),
                    ),
                    Stack(1),
                    span,
                );
                let counter =
                    self.builder
                        .append(block, Ir::Constant(0, DefId::unit()), Stack(1), span);

                let iter_offset = block.current_offset();
                let elem_rel_local = self.builder.top_plus(1);
                let elem_val_local = self.builder.top_plus(2);

                let iter_body_index = {
                    let mut iter_block = self.builder.new_block(Stack(2), span);

                    self.gen_in_scope(
                        &[
                            (out_seq, iter_binder.seq),
                            (elem_rel_local, iter_binder.rel),
                            (elem_val_local, iter_binder.val),
                        ],
                        nodes.into_iter(),
                        &mut iter_block,
                    );

                    self.builder
                        .append_pop_until(&mut iter_block, counter, span);

                    self.builder
                        .commit(iter_block, Terminator::PopGoto(block.index(), iter_offset))
                };

                self.builder.append(
                    block,
                    Ir::Iter(seq_local, counter, iter_body_index),
                    Stack(0),
                    span,
                );

                self.builder.append_pop_until(block, out_seq, span);
            }
            NodeKind::Iter(..) => {
                todo!("iter");
            }
            NodeKind::Push(seq_var, attr) => {
                let top = self.builder.top();
                let seq_local = self.var_local(seq_var);
                self.gen_node(*attr.rel, block);
                let rel_local = self.builder.top();

                self.gen_node(*attr.val, block);

                self.builder
                    .append(block, Ir::Clone(rel_local), Stack(1), span);

                self.builder
                    .append(block, Ir::AppendAttr2(seq_local), Stack(-2), span);

                self.builder.append_pop_until(block, top, span);
            }
        }
    }

    fn gen_in_scope<'m>(
        &mut self,
        scopes: &[(Local, PatternBinding)],
        nodes: impl Iterator<Item = TypedHirNode<'m>>,
        block: &mut Block,
    ) {
        for (local, binding) in scopes {
            if let PatternBinding::Binder(var) = binding {
                if self.scope.insert(*var, *local).is_some() {
                    panic!("Variable {var} already in scope");
                }
            }
        }

        for node in nodes {
            self.gen_node(node, block);
        }

        for (_, binding) in scopes {
            if let PatternBinding::Binder(var) = binding {
                self.scope.remove(var);
            }
        }
    }

    fn var_local(&self, var: Variable) -> Local {
        *self
            .scope
            .get(&var)
            .unwrap_or_else(|| panic!("Variable {var} not in scope"))
    }
}
