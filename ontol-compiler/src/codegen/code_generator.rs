use fnv::FnvHashMap;
use ontol_runtime::{
    vm::proc::{BuiltinProc, Local, NParams, Predicate, Procedure},
    DefId,
};
use tracing::debug;

use crate::{
    codegen::{ir::Terminator, proc_builder::Stack},
    error::CompileError,
    typed_hir::{HirFunc, TypedHir, TypedHirNode},
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
    let mut block = builder.new_block(Stack(0), expr.span());
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

    let return_ty = func.body.ty();

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Stack(1), func.body.span());
    let mut generator = CodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
        errors,
    };
    generator.scope.insert(func.arg.var, Local(0));
    generator.gen_node(func.body, &mut block);
    builder.commit(block, Terminator::Return(builder.top()));

    match (find_mapping_key(func.arg.ty), find_mapping_key(return_ty)) {
        (Some(from_def), Some(to_def)) => {
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

    scope: FnvHashMap<ontol_hir::Var, Local>,
}

impl<'a> CodeGenerator<'a> {
    fn gen_node(&mut self, TypedHirNode(kind, meta): TypedHirNode, block: &mut Block) {
        let ty = meta.ty;
        let span = meta.span;
        match kind {
            ontol_hir::Kind::Var(var) => match self.scope.get(&var) {
                Some(local) => {
                    self.builder
                        .append(block, Ir::Clone(*local), Stack(1), span);
                }
                None => {
                    self.errors.push(SpannedCompileError {
                        error: CompileError::UnboundVariable,
                        span,
                    });
                }
            },
            ontol_hir::Kind::Unit => {
                self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                    Stack(1),
                    span,
                );
            }
            ontol_hir::Kind::Int(int) => {
                self.builder.append(
                    block,
                    Ir::Constant(int, ty.get_single_def_id().unwrap()),
                    Stack(1),
                    span,
                );
            }
            ontol_hir::Kind::Let(binder, definition, body) => {
                self.gen_node(*definition, block);
                self.scope.insert(binder.0, self.builder.top());
                for node in body {
                    self.gen_node(node, block);
                }
                self.scope.remove(&binder.0);
            }
            ontol_hir::Kind::Call(proc, params) => {
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
            ontol_hir::Kind::Map(param) => {
                let from = find_mapping_key(param.ty()).unwrap();
                let to = find_mapping_key(ty).unwrap();

                self.gen_node(*param, block);

                let proc = Procedure {
                    address: self.proc_table.gen_mapping_addr(from, to),
                    n_params: NParams(1),
                };

                self.builder.append(block, Ir::Call(proc), Stack(0), span);
            }
            ontol_hir::Kind::Seq(_label, _attr) => {
                todo!("seq");
            }
            ontol_hir::Kind::Struct(binder, nodes) => {
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
            ontol_hir::Kind::Prop(_, struct_var, id, variants) => {
                if let Some(ontol_hir::PropVariant { dimension: _, attr }) =
                    variants.into_iter().next()
                {
                    // FIXME: Don't ignore relation parameters!
                    // self.generate(*variant.attr.rel, block);
                    // let rel = self.builder.top();
                    self.gen_node(*attr.val, block);

                    let struct_local = self.var_local(struct_var);

                    self.builder
                        .append(block, Ir::PutAttrValue(struct_local, id), Stack(-1), span);
                }
            }
            ontol_hir::Kind::MatchProp(struct_var, id, arms) => {
                let struct_local = self.var_local(struct_var);

                if arms.len() > 1 {
                    if arms
                        .iter()
                        .any(|arm| matches!(arm.pattern, ontol_hir::PropPattern::Absent))
                    {
                        self.builder.append(
                            block,
                            Ir::TryTakeAttr2(struct_local, id),
                            // even if three locals are pushed, the `status` one
                            // is not kept track of here.
                            Stack(2),
                            span,
                        );

                        let status_local = self.builder.top_minus(1);
                        let post_cond_offset = block.current_offset().plus(1);

                        // These overlaps with the status_local, but that will be removed in the Cond opcode,
                        // leading into the present block.
                        let val_local = self.builder.top();
                        let rel_local = self.builder.top_minus(1);

                        let present_body_index = {
                            let mut present_block = self.builder.new_block(Stack(0), span);

                            for arm in arms {
                                if !matches!(arm.pattern, ontol_hir::PropPattern::Absent) {
                                    self.gen_match_arm(
                                        arm,
                                        (rel_local, val_local),
                                        &mut present_block,
                                    );
                                }
                            }

                            self.builder.commit(
                                present_block,
                                Terminator::PopGoto(block.index(), post_cond_offset),
                            )
                        };

                        self.builder.append(
                            block,
                            Ir::Cond(Predicate::YankTrue(status_local), present_body_index),
                            Stack(0),
                            span,
                        );
                    } else {
                        todo!();
                    }
                } else {
                    self.builder
                        .append(block, Ir::TakeAttr2(struct_local, id), Stack(2), span);

                    let val_local = self.builder.top();
                    let rel_local = self.builder.top_minus(1);

                    for arm in arms {
                        self.gen_match_arm(arm, (rel_local, val_local), block);
                    }
                }
            }
            ontol_hir::Kind::Gen(seq_var, iter_binder, nodes) => {
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
            ontol_hir::Kind::Iter(..) => {
                todo!("iter");
            }
            ontol_hir::Kind::Push(seq_var, attr) => {
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

    fn gen_match_arm(
        &mut self,
        arm: ontol_hir::MatchArm<TypedHir>,
        (rel_local, val_local): (Local, Local),
        block: &mut Block,
    ) {
        match arm.pattern {
            ontol_hir::PropPattern::Attr(rel_binding, val_binding) => self.gen_in_scope(
                &[(rel_local, rel_binding), (val_local, val_binding)],
                arm.nodes.into_iter(),
                block,
            ),
            ontol_hir::PropPattern::Seq(seq_binding) => self.gen_in_scope(
                &[
                    (rel_local, ontol_hir::Binding::Wildcard),
                    (val_local, seq_binding),
                ],
                arm.nodes.into_iter(),
                block,
            ),
            ontol_hir::PropPattern::Absent => {
                todo!("Arm pattern not present")
            }
        }
    }

    fn gen_in_scope<'m>(
        &mut self,
        scopes: &[(Local, ontol_hir::Binding)],
        nodes: impl Iterator<Item = TypedHirNode<'m>>,
        block: &mut Block,
    ) {
        for (local, binding) in scopes {
            if let ontol_hir::Binding::Binder(var) = binding {
                if self.scope.insert(*var, *local).is_some() {
                    panic!("Variable {var} already in scope");
                }
            }
        }

        for node in nodes {
            self.gen_node(node, block);
        }

        for (_, binding) in scopes {
            if let ontol_hir::Binding::Binder(var) = binding {
                self.scope.remove(var);
            }
        }
    }

    fn var_local(&self, var: ontol_hir::Var) -> Local {
        *self
            .scope
            .get(&var)
            .unwrap_or_else(|| panic!("Variable {var} not in scope"))
    }
}
