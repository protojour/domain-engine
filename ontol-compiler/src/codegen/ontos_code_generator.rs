use fnv::FnvHashMap;
use ontol_runtime::{
    vm::proc::{BuiltinProc, Local, NParams, Procedure},
    DefId,
};
use ontos::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern},
    Variable,
};
use tracing::debug;

use crate::{
    codegen::{ir::Terminator, proc_builder::Stack},
    typed_ontos::lang::{OntosFunc, OntosNode, TypedOntos},
};

use super::{
    ir::Ir,
    proc_builder::{Block, ProcBuilder},
    task::{find_mapping_key, ProcTable},
};

pub(super) fn map_codegen_ontos(proc_table: &mut ProcTable, func: OntosFunc) -> bool {
    debug!("Generating code for\n{}", func.body);

    let mut builder = ProcBuilder::new(NParams(0));
    let mut block = builder.new_block(Stack(1), func.body.meta.span);
    let mut generator = OntosCodeGenerator {
        proc_table,
        builder: &mut builder,
        scope: Default::default(),
    };
    generator.scope.insert(func.arg.0, Local(0));
    generator.gen_node(func.body, &mut block);
    builder.commit(block, Terminator::Return(builder.top()));

    false
}

pub(super) struct OntosCodeGenerator<'a> {
    proc_table: &'a mut ProcTable,
    pub builder: &'a mut ProcBuilder,

    scope: FnvHashMap<Variable, Local>,
}

impl<'a> OntosCodeGenerator<'a> {
    fn gen_node(&mut self, node: OntosNode, block: &mut Block) {
        match node.kind {
            NodeKind::VariableRef(var) => {
                let local = self.var_local(var);
                self.builder
                    .append(block, Ir::Clone(local), Stack(1), node.meta.span);
            }
            NodeKind::Unit => {
                self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewUnit, DefId::unit()),
                    Stack(1),
                    node.meta.span,
                );
            }
            NodeKind::Int(int) => {
                self.builder.append(
                    block,
                    Ir::Constant(int, node.meta.ty.get_single_def_id().unwrap()),
                    Stack(1),
                    node.meta.span,
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
                let return_def_id = node.meta.ty.get_single_def_id().unwrap();
                self.builder.append(
                    block,
                    Ir::CallBuiltin(proc, return_def_id),
                    stack_delta,
                    node.meta.span,
                );
            }
            NodeKind::Map(param) => {
                let from = find_mapping_key(param.meta.ty).unwrap();
                let to = find_mapping_key(node.meta.ty).unwrap();

                self.gen_node(*param, block);

                let proc = Procedure {
                    address: self.proc_table.gen_mapping_addr(from, to),
                    n_params: NParams(1),
                };

                self.builder
                    .append(block, Ir::Call(proc), Stack(0), node.meta.span);
            }
            NodeKind::Seq(_label, _attr) => {
                todo!("seq");
            }
            NodeKind::Struct(binder, nodes) => {
                let def_id = node.meta.ty.get_single_def_id().unwrap();
                let local = self.builder.append(
                    block,
                    Ir::CallBuiltin(BuiltinProc::NewMap, def_id),
                    Stack(1),
                    node.meta.span,
                );
                self.scope.insert(binder.0, local);
                for node in nodes {
                    self.gen_node(node, block);
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

                    self.builder.append(
                        block,
                        Ir::PutAttrValue(struct_local, id),
                        Stack(-1),
                        node.meta.span,
                    );
                }
            }
            NodeKind::MapSeq(..) => {
                unimplemented!("map-seq");
            }
            NodeKind::MatchProp(struct_var, id, arms) => {
                let struct_local = self.var_local(struct_var);
                let stack_size = self.builder.top();

                self.builder.append(
                    block,
                    Ir::TakeAttr2(struct_local, id),
                    Stack(2),
                    node.meta.span,
                );
                let value_local = self.builder.top();
                let rel_local = self.builder.top_minus(1);

                if arms.len() != 1 {
                    todo!("multiple arms");
                }

                for arm in arms {
                    match arm.pattern {
                        PropPattern::Present(seq, rel_binding, val_binding) => match seq {
                            Some(_seq) => {
                                // todo!("seq");
                            }
                            None => {
                                if let PatternBinding::Binder(var) = rel_binding {
                                    self.scope.insert(var, rel_local);
                                }
                                if let PatternBinding::Binder(var) = val_binding {
                                    self.scope.insert(var, value_local);
                                }

                                for node in arm.nodes {
                                    self.gen_node(node, block);
                                }

                                if let PatternBinding::Binder(var) = rel_binding {
                                    self.scope.remove(&var);
                                }
                                if let PatternBinding::Binder(var) = val_binding {
                                    self.scope.remove(&var);
                                }
                            }
                        },
                        PropPattern::NotPresent => {
                            todo!("Arm pattern not present")
                        }
                    }
                }

                self.builder
                    .append_pop_until(block, stack_size, node.meta.span);
            }
        }
    }

    fn var_local(&self, var: Variable) -> Local {
        *self.scope.get(&var).expect("Variable not in scope")
    }
}
