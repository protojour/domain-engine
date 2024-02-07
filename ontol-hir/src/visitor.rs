use ontol_runtime::value::PropertyId;

use crate::{
    arena::{Arena, NodeRef},
    Attribute, Binding, Iter, Kind, Label, Lang, Node, PredicateClosure, PropFlags, PropVariant,
    SetEntry, Var,
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
        prop_id: PropertyId,
        variants: &[PropVariant<'a, L>],
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_prop(struct_var, prop_id, variants, arena);
    }

    #[allow(unused_variables)]
    fn visit_prop_variant(
        &mut self,
        index: usize,
        variant: &PropVariant<'a, L>,
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_prop_variant(variant, arena);
    }

    #[allow(unused_variables)]
    fn visit_seq_prop_element(
        &mut self,
        index: usize,
        element: &(Iter, Attribute<Node>),
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_seq_prop_element(index, element, arena);
    }

    #[allow(unused_variables)]
    fn visit_set_entry(&mut self, index: usize, entry: &SetEntry<'a, L>, arena: &'h Arena<'a, L>) {
        self.traverse_set_entry(index, entry, arena);
    }

    #[allow(unused_variables)]
    fn visit_pattern_binding(&mut self, index: usize, binding: &Binding<'a, L>) {
        self.traverse_pattern_binding(binding);
    }

    #[allow(unused_variables)]
    fn visit_property_id(&mut self, prop_id: PropertyId) {}

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
            Kind::Catch(label, nodes) => {
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
            Kind::LetProp(Attribute { rel, val }, (var, _prop_id)) => {
                self.traverse_pattern_binding(rel);
                self.traverse_pattern_binding(val);
                self.visit_var(*var);
            }
            Kind::LetPropDefault(binding, (var, _prop_id), default) => {
                self.traverse_pattern_binding(&binding.rel);
                self.traverse_pattern_binding(&binding.val);
                self.visit_var(*var);
                self.visit_node(0, arena.node_ref(default.rel));
                self.visit_node(1, arena.node_ref(default.val));
            }
            Kind::TryLetProp(try_label, Attribute { rel, val }, (var, _prop_id)) => {
                self.visit_label(*try_label);
                self.traverse_pattern_binding(rel);
                self.traverse_pattern_binding(val);
                self.visit_var(*var);
            }
            Kind::TryLetTup(try_label, bindings, node) => {
                self.visit_label(*try_label);
                for (index, binding) in bindings.iter().enumerate() {
                    self.visit_pattern_binding(index, binding);
                }
                self.visit_node(0, arena.node_ref(*node));
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
            Kind::Map(arg) => {
                self.visit_node(0, arena.node_ref(*arg));
            }
            Kind::With(binder, def, body) => {
                self.visit_binder(L::as_hir(binder).var);
                self.visit_node(0, arena.node_ref(*def));
                for (index, node_ref) in arena.node_refs(body).enumerate() {
                    self.visit_node(index + 1, node_ref);
                }
            }
            Kind::Set(entries) => {
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
            Kind::Prop(optional, struct_var, prop_id, variants) => {
                self.visit_prop(*optional, *struct_var, *prop_id, variants, arena);
            }
            Kind::MoveRestAttrs(target, source) => {
                self.visit_var(*target);
                self.visit_var(*source);
            }
            Kind::MakeSeq(binder, children) => {
                self.visit_binder(L::as_hir(binder).var);
                for (index, child) in arena.node_refs(children).enumerate() {
                    self.visit_node(index, child);
                }
            }
            Kind::CopySubSeq(target, source) => {
                self.visit_var(*target);
                self.visit_var(*source);
            }
            Kind::ForEach(seq_var, (rel, val), body) => {
                self.visit_var(*seq_var);
                self.traverse_pattern_binding(rel);
                self.traverse_pattern_binding(val);
                for (index, node_ref) in arena.node_refs(body).enumerate() {
                    self.visit_node(index, node_ref);
                }
            }
            Kind::Insert(seq_var, attr) => {
                self.visit_var(*seq_var);
                self.visit_node(0, arena.node_ref(attr.rel));
                self.visit_node(1, arena.node_ref(attr.val));
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
            Kind::PushCondClause(var, _) => {
                self.visit_var(*var);
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_prop(
        &mut self,
        struct_var: Var,
        prop_id: PropertyId,
        variants: &[PropVariant<'a, L>],
        arena: &'h Arena<'a, L>,
    ) {
        self.visit_var(struct_var);
        self.visit_property_id(prop_id);
        for (index, variant) in variants.iter().enumerate() {
            self.visit_prop_variant(index, variant, arena);
        }
    }

    fn traverse_prop_variant(&mut self, variant: &PropVariant<'a, L>, arena: &'h Arena<'a, L>) {
        match variant {
            PropVariant::Singleton(attr) => {
                self.visit_node(0, arena.node_ref(attr.rel));
                self.visit_node(1, arena.node_ref(attr.val));
            }
            PropVariant::Set(seq_variant) => {
                self.visit_label(*L::as_hir(&seq_variant.label));
                for (index, element) in seq_variant.elements.iter().enumerate() {
                    self.visit_seq_prop_element(index, element, arena);
                }
            }
            PropVariant::Predicate(closure) => match closure {
                PredicateClosure::ContainsElement(attr) => {
                    self.visit_node(0, arena.node_ref(attr.rel));
                    self.visit_node(1, arena.node_ref(attr.val));
                }
                PredicateClosure::ElementIn(node)
                | PredicateClosure::AllInSet(node)
                | PredicateClosure::SetContainsAll(node)
                | PredicateClosure::SetIntersects(node)
                | PredicateClosure::SetEquals(node) => {
                    self.visit_node(0, arena.node_ref(*node));
                }
            },
        }
    }

    #[allow(unused_variables)]
    fn traverse_seq_prop_element(
        &mut self,
        index: usize,
        element: &(Iter, Attribute<Node>),
        arena: &'h Arena<'a, L>,
    ) {
        self.visit_node(0, arena.node_ref(element.1.rel));
        self.visit_node(1, arena.node_ref(element.1.val));
    }

    #[allow(unused_variables)]
    fn traverse_set_entry(
        &mut self,
        index: usize,
        entry: &SetEntry<'a, L>,
        arena: &'h Arena<'a, L>,
    ) {
        self.visit_node(0, arena.node_ref(entry.1.rel));
        self.visit_node(1, arena.node_ref(entry.1.val));
    }

    fn traverse_pattern_binding(&mut self, binding: &Binding<'a, L>) {
        match binding {
            Binding::Binder(binder) => self.visit_binder(L::as_hir(binder).var),
            Binding::Wildcard => {}
        }
    }
}
