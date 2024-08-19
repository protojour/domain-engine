use ontol_runtime::PropId;

use crate::{
    arena::{Arena, NodeRef},
    Binding, Kind, Label, Lang, MatrixRow, Node, Pack, PropFlags, PropVariant, Var,
};

pub trait HirVisitor<'h, 'a: 'h, L: Lang + 'h> {
    #[allow(unused_variables)]
    fn visit_node(&mut self, index: usize, node_ref: NodeRef<'h, 'a, L>) {
        self.traverse_node(node_ref);
    }

    #[allow(unused_variables)]
    fn visit_prop(
        &mut self,
        flags: PropFlags,
        struct_var: Var,
        prop_id: PropId,
        variant: &PropVariant,
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_prop(struct_var, prop_id, variant, arena);
    }

    #[allow(unused_variables)]
    fn visit_prop_variant(&mut self, variant: &PropVariant, arena: &'h Arena<'a, L>) {
        self.traverse_prop_variant(variant, arena);
    }

    #[allow(unused_variables)]
    fn visit_set_entry(&mut self, index: usize, entry: &MatrixRow<'a, L>, arena: &'h Arena<'a, L>) {
        self.traverse_set_entry(index, entry, arena);
    }

    #[allow(unused_variables)]
    fn visit_pattern_binding(&mut self, index: usize, binding: &Binding<'a, L>) {
        self.traverse_pattern_binding(binding);
    }

    #[allow(unused_variables)]
    fn visit_property_id(&mut self, prop_id: PropId) {}

    #[allow(unused_variables)]
    fn visit_binder(&mut self, var: Var) {}

    #[allow(unused_variables)]
    fn visit_var(&mut self, var: Var) {}

    #[allow(unused_variables)]
    fn visit_label(&mut self, label: Label) {}

    fn traverse_node(&mut self, node_ref: NodeRef<'h, 'a, L>) {
        let arena = node_ref.arena;
        match node_ref.kind() {
            Kind::NoOp => {}
            Kind::Var(var) => {
                self.visit_var(*var);
            }
            Kind::Block(nodes) => {
                for (index, node) in arena.node_refs(nodes).enumerate() {
                    self.visit_node(index, node)
                }
            }
            Kind::Catch(label, nodes) | Kind::CatchFunc(label, nodes) => {
                self.visit_label(*label);
                for (index, node) in arena.node_refs(nodes).enumerate() {
                    self.visit_node(index, node)
                }
            }
            Kind::Try(label, var) => {
                self.visit_label(*label);
                self.visit_var(*var);
            }
            Kind::Let(binder, node) => {
                self.visit_binder(L::as_hir(binder).var);
                self.visit_node(0, arena.node_ref(*node));
            }
            Kind::TryLet(label, binder, node) => {
                self.visit_label(*label);
                self.visit_binder(L::as_hir(binder).var);
                self.visit_node(0, arena.node_ref(*node));
            }
            Kind::LetProp(bind_pack, (var, _prop_id)) => {
                self.traverse_pack_binding(bind_pack);
                self.visit_var(*var);
            }
            Kind::LetPropDefault(bind_pack, (var, _prop_id), default) => {
                self.traverse_pack_binding(bind_pack);
                self.visit_var(*var);
                for (i, node) in default.iter().enumerate() {
                    self.visit_node(i, arena.node_ref(*node));
                }
            }
            Kind::TryLetProp(try_label, bind_pack, (var, _prop_id)) => {
                self.visit_label(*try_label);
                self.traverse_pack_binding(bind_pack);
                self.visit_var(*var);
            }
            Kind::TryLetTup(try_label, bindings, node) => {
                self.visit_label(*try_label);
                for (index, binding) in bindings.iter().enumerate() {
                    self.visit_pattern_binding(index, binding);
                }
                self.visit_node(0, arena.node_ref(*node));
            }
            Kind::TryNarrow(try_label, var) => {
                self.visit_label(*try_label);
                self.visit_var(*var);
            }
            Kind::LetRegex(groups_list, _regex_def_id, var) => {
                for capture_groups in groups_list.iter() {
                    for capture_group in capture_groups.iter() {
                        self.visit_binder(L::as_hir(&capture_group.binder).var);
                    }
                }
                self.visit_var(*var);
            }
            Kind::LetRegexIter(binder, groups_list, _regex_def_id, var) => {
                self.visit_binder(L::as_hir(binder).var);
                for capture_groups in groups_list.iter() {
                    for capture_group in capture_groups.iter() {
                        self.visit_binder(L::as_hir(&capture_group.binder).var);
                    }
                }
                self.visit_var(*var);
            }
            Kind::Unit | Kind::I64(_) | Kind::F64(_) | Kind::Text(_) | Kind::Const(_) => {}
            Kind::Call(_proc, params) => {
                for (index, arg) in arena.node_refs(params).enumerate() {
                    self.visit_node(index, arg);
                }
            }
            Kind::Map(arg) | Kind::Pun(arg) => {
                self.visit_node(0, arena.node_ref(*arg));
            }
            Kind::Narrow(arg) => {
                self.visit_node(0, arena.node_ref(*arg));
            }
            Kind::With(binder, def, body) => {
                self.visit_binder(L::as_hir(binder).var);
                self.visit_node(0, arena.node_ref(*def));
                for (index, node_ref) in arena.node_refs(body).enumerate() {
                    self.visit_node(index + 1, node_ref);
                }
            }
            Kind::Matrix(entries) => {
                for (index, entry) in entries.iter().enumerate() {
                    self.visit_set_entry(index, entry, arena);
                }
            }
            Kind::Struct(binder, _flags, children) => {
                self.visit_binder(L::as_hir(binder).var);
                for (index, child) in arena.node_refs(children).enumerate() {
                    self.visit_node(index, child);
                }
            }
            Kind::Prop(optional, struct_var, prop_id, variant) => {
                self.visit_prop(*optional, *struct_var, *prop_id, variant, arena);
            }
            Kind::MoveRestAttrs(target, source) => {
                self.visit_var(*target);
                self.visit_var(*source);
            }
            Kind::MakeSeq(binder, children) => {
                if let Some(binder) = binder.as_ref() {
                    self.visit_binder(L::as_hir(binder).var);
                }
                for (index, child) in arena.node_refs(children).enumerate() {
                    self.visit_node(index, child);
                }
            }
            Kind::MakeMatrix(binders, children) => {
                for binder in binders {
                    self.visit_binder(L::as_hir(binder).var);
                }
                for (index, child) in arena.node_refs(children).enumerate() {
                    self.visit_node(index, child);
                }
            }
            Kind::CopySubSeq(target, source) => {
                self.visit_var(*target);
                self.visit_var(*source);
            }
            Kind::ForEach(elements, body) => {
                for (seq_var, binding) in elements {
                    self.visit_var(*seq_var);
                    self.traverse_pattern_binding(binding);
                }
                for (index, node_ref) in arena.node_refs(body).enumerate() {
                    self.visit_node(index, node_ref);
                }
            }
            Kind::Insert(seq_var, node) => {
                self.visit_var(*seq_var);
                self.visit_node(0, arena.node_ref(*node));
            }
            Kind::StringPush(to_var, node) => {
                self.visit_var(*to_var);
                self.visit_node(0, arena.node_ref(*node));
            }
            Kind::Regex(label, _, capture_groups_list) => {
                if let Some(label) = label {
                    self.visit_label(*L::as_hir(label));
                }

                for capture_groups in capture_groups_list.iter() {
                    for capture_group in capture_groups.iter() {
                        self.visit_binder(L::as_hir(&capture_group.binder).var);
                    }
                }
            }
            Kind::LetCondVar(bind_var, cond) => {
                self.visit_binder(*bind_var);
                self.visit_var(*cond);
            }
            Kind::PushCondClauses(var, _clauses) => {
                self.visit_var(*var);
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_prop(
        &mut self,
        struct_var: Var,
        prop_id: PropId,
        variant: &PropVariant,
        arena: &'h Arena<'a, L>,
    ) {
        self.visit_var(struct_var);
        self.visit_property_id(prop_id);
        self.visit_prop_variant(variant, arena);
    }

    fn traverse_prop_variant(&mut self, variant: &PropVariant, arena: &'h Arena<'a, L>) {
        match variant {
            PropVariant::Unit(node) => {
                self.visit_node(0, arena.node_ref(*node));
            }
            PropVariant::Tuple(tup) => {
                for (i, node) in tup.iter().enumerate() {
                    self.visit_node(i, arena.node_ref(*node));
                }
            }
            PropVariant::Predicate(_operator, node) => {
                self.visit_node(0, arena.node_ref(*node));
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_set_entry(
        &mut self,
        index: usize,
        MatrixRow(iter_label, elements): &MatrixRow<'a, L>,
        arena: &'h Arena<'a, L>,
    ) {
        if let Some(label_data) = iter_label {
            self.visit_label(*L::as_hir(label_data));
        }

        for (index, element) in elements.iter().enumerate() {
            self.visit_node(index, arena.node_ref(*element));
        }
    }

    fn traverse_pack_binding(&mut self, pack: &Pack<Binding<'a, L>>) {
        match pack {
            Pack::Unit(u) => {
                self.traverse_pattern_binding(u);
            }
            Pack::Tuple(t) => {
                for el in t {
                    self.traverse_pattern_binding(el);
                }
            }
        }
    }

    fn traverse_pattern_binding(&mut self, binding: &Binding<'a, L>) {
        match binding {
            Binding::Binder(binder) => self.visit_binder(L::as_hir(binder).var),
            Binding::Wildcard => {}
        }
    }

    fn traverse_pack_node(&mut self, pack: &Pack<Node>, arena: &'h Arena<'a, L>) {
        match pack {
            Pack::Unit(u) => {
                self.visit_node(0, arena.node_ref(*u));
            }
            Pack::Tuple(t) => {
                for (i, node) in t.iter().enumerate() {
                    self.visit_node(i, arena.node_ref(*node));
                }
            }
        }
    }
}
