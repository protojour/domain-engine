use fnv::FnvHashMap;
use ontol_runtime::{
    vm::proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use ontos::{
    kind::{NodeKind, PatternBinding, PropPattern},
    Variable,
};
use tracing::debug;

use crate::{
    codegen::{ir::Terminator, proc_builder::Stack},
    typed_ontos::lang::{OntosFunc, OntosNode},
    types::Type,
    IrVariant, CODE_GENERATOR,
};

use super::{
    ir::Ir,
    proc_builder::{Block, ProcBuilder},
    task::{find_mapping_key, ProcTable},
};

pub(super) fn map_codegen_ontos(proc_table: &mut ProcTable, func: OntosFunc) -> bool {
    debug!("Generating code for\n{}", func);

    let return_ty = func.body.meta.ty;

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Stack(1), func.body.meta.span);
    let mut generator = OntosCodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
    };
    generator.scope.insert(func.arg.variable, Local(0));
    generator.gen_node(func.body, &mut block);
    builder.commit(block, Terminator::Return(builder.top()));

    match (find_mapping_key(func.arg.ty), find_mapping_key(return_ty)) {
        (Some(from_def), Some(to_def)) => {
            debug!("Saving proc: {:?} -> {:?}", func.arg.ty, return_ty);

            if CODE_GENERATOR == IrVariant::Ontos {
                proc_table
                    .map_procedures
                    .insert((from_def, to_def), builder);
            }
            true
        }
        (from_def, to_def) => {
            if CODE_GENERATOR == IrVariant::Ontos {
                panic!("Problem finding def ids: ({from_def:?}, {to_def:?})");
            }
            false
        }
    }
}

pub(super) struct OntosCodeGenerator<'a> {
    proc_table: &'a mut ProcTable,
    pub builder: &'a mut ProcBuilder,

    scope: FnvHashMap<Variable, Local>,
}

impl<'a> OntosCodeGenerator<'a> {
    fn gen_node(&mut self, node: OntosNode, block: &mut Block) {
        let ty = node.meta.ty;
        let span = node.meta.span;
        match node.kind {
            NodeKind::VariableRef(var) => {
                let local = self.var_local(var);
                self.builder.append(block, Ir::Clone(local), Stack(1), span);
            }
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
                    Ir::CallBuiltin(BuiltinProc::NewMap, def_id),
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
                if let Some(variant) = variants.into_iter().next() {
                    // FIXME: Don't ignore relation parameters!
                    // self.generate(*variant.attr.rel, block);
                    // let rel = self.builder.top();
                    self.gen_node(*variant.attr.val, block);

                    let struct_local = self.var_local(struct_var);

                    self.builder
                        .append(block, Ir::PutAttrValue(struct_local, id), Stack(-1), span);
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
                        PropPattern::Attr(rel_binding, val_binding) => {
                            self.gen_match_arm(
                                (rel_local, rel_binding),
                                (val_local, val_binding),
                                arm.nodes,
                                block,
                            );
                        }
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

                                self.gen_match_arm(
                                    (elem_rel_local, rel_binding),
                                    (elem_val_local, val_binding),
                                    arm.nodes,
                                    &mut iter_block,
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

                            // todo!("seq");
                        }
                        PropPattern::Seq(_) => {
                            todo!("Seq present")
                        }
                        PropPattern::Absent => {
                            todo!("Arm pattern not present")
                        }
                    }
                }
            }
            NodeKind::Gen(..) => {
                todo!("gen");
            }
            NodeKind::Iter(..) => {
                todo!("iter");
            }
            NodeKind::Push(..) => {
                todo!("push");
            }
        }
    }

    fn gen_match_arm(
        &mut self,
        (rel_local, rel_binding): (Local, PatternBinding),
        (val_local, val_binding): (Local, PatternBinding),
        nodes: Vec<OntosNode>,
        block: &mut Block,
    ) {
        if let PatternBinding::Binder(var) = rel_binding {
            self.scope.insert(var, rel_local);
        }
        if let PatternBinding::Binder(var) = val_binding {
            self.scope.insert(var, val_local);
        }

        for node in nodes {
            self.gen_node(node, block);
        }

        if let PatternBinding::Binder(var) = rel_binding {
            self.scope.remove(&var);
        }
        if let PatternBinding::Binder(var) = val_binding {
            self.scope.remove(&var);
        }
    }

    fn var_local(&self, var: Variable) -> Local {
        *self.scope.get(&var).expect("Variable not in scope")
    }
}
