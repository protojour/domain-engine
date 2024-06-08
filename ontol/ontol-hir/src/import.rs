use ontol_runtime::value::Attribute;
use smallvec::SmallVec;

use crate::{
    arena::{Arena, NodeRef},
    Lang, Node, Nodes, PropVariant, SetEntry,
};

/// Import from one arena into another
pub fn arena_import<'a, L: Lang>(target: &mut Arena<'a, L>, source: NodeRef<'_, 'a, L>) -> Node {
    let mut importer = Importer {
        source: source.arena(),
        target,
    };

    importer.import(source.node())
}

struct Importer<'i, 'o, 'a, L: Lang> {
    source: &'i Arena<'a, L>,
    target: &'o mut Arena<'a, L>,
}

impl<'i, 'o, 'a, L: Lang> Importer<'i, 'o, 'a, L> {
    fn import(&mut self, node: Node) -> Node {
        let data = &self.source[node];
        let kind = L::as_hir(data);

        use crate::Kind::*;

        let imported_kind = match kind {
            NoOp | Var(_) | Unit | I64(_) | F64(_) | Text(_) | Const(_) | CopySubSeq(..)
            | MoveRestAttrs(..) => {
                // No subnodes
                kind.clone()
            }
            Block(body) => Block(self.import_nodes(body)),
            Catch(label, body) => Catch(*label, self.import_nodes(body)),
            CatchFunc(label, body) => CatchFunc(*label, self.import_nodes(body)),
            Try(label, var) => Try(*label, *var),
            TryNarrow(label, var) => TryNarrow(*label, *var),
            Let(binder, node) => Let(binder.clone(), self.import(*node)),
            TryLet(label, binder, node) => TryLet(*label, binder.clone(), self.import(*node)),
            LetProp(binding, (var, prop_id)) => LetProp(binding.clone(), (*var, *prop_id)),
            LetPropDefault(binding, (var, prop_id), default) => LetPropDefault(
                binding.clone(),
                (*var, *prop_id),
                default.iter().map(|node| self.import(*node)).collect(),
            ),
            TryLetProp(catch, attr, (var, prop_id)) => {
                TryLetProp(*catch, attr.clone(), (*var, *prop_id))
            }
            TryLetTup(catch, bindings, source) => TryLetTup(*catch, bindings.clone(), *source),
            LetRegex(groups_list, regex_def_id, var) => {
                LetRegex(groups_list.clone(), *regex_def_id, *var)
            }
            LetRegexIter(binding, groups_list, regex_def_id, var) => {
                LetRegexIter(binding.clone(), groups_list.clone(), *regex_def_id, *var)
            }
            With(binder, def, body) => {
                With(binder.clone(), self.import(*def), self.import_nodes(body))
            }
            Call(proc, args) => Call(*proc, self.import_nodes(args)),
            Map(arg) => Map(self.import(*arg)),
            Pun(arg) => Pun(self.import(*arg)),
            Narrow(arg) => Narrow(self.import(*arg)),
            Set(entries) => Set(self.import_entries(entries)),
            Struct(binder, flags, body) => Struct(binder.clone(), *flags, self.import_nodes(body)),
            Prop(optional, struct_var, prop_id, variant) => Prop(
                *optional,
                *struct_var,
                *prop_id,
                match variant {
                    PropVariant::Unit(node) => PropVariant::Unit(self.import(*node)),
                    PropVariant::Tuple(tup) => PropVariant::Tuple(self.import_nodes(tup)),
                    PropVariant::Predicate(operator, param) => {
                        PropVariant::Predicate(*operator, self.import(*param))
                    }
                },
            ),
            MakeSeq(binder, body) => MakeSeq(binder.clone(), self.import_nodes(body)),
            ForEach(var, (rel, val), body) => {
                ForEach(*var, (rel.clone(), val.clone()), self.import_nodes(body))
            }
            Insert(var, attr) => Insert(*var, self.import_attr(*attr)),
            StringPush(var, node) => StringPush(*var, self.import(*node)),
            Regex(label, def_id, groups_list) => Regex(label.clone(), *def_id, groups_list.clone()),
            LetCondVar(bind_var, cond) => LetCondVar(*bind_var, *cond),
            PushCondClauses(var, clauses) => PushCondClauses(*var, clauses.clone()),
        };

        self.target.add(L::wrap(data, imported_kind))
    }

    fn import_nodes(&mut self, nodes: &[Node]) -> Nodes {
        let mut imported_nodes = Nodes::default();
        for node in nodes {
            imported_nodes.push(self.import(*node));
        }
        imported_nodes
    }

    fn import_attr(&mut self, attr: Attribute<Node>) -> Attribute<Node> {
        let rel = self.import(attr.rel);
        let val = self.import(attr.val);
        Attribute { rel, val }
    }

    fn import_entries(&mut self, entries: &[SetEntry<'a, L>]) -> SmallVec<SetEntry<'a, L>, 1> {
        entries
            .iter()
            .map(|SetEntry(iter, attr)| SetEntry(iter.clone(), self.import_attr(*attr)))
            .collect()
    }
}
