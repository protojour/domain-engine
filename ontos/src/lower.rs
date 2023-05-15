use std::collections::HashMap;

use super::{
    ast::{Hir2Ast, Hir2AstVariable},
    visitor::Hir2AstVisitor,
};
use ontol_compiler::ontos::{node::Hir2Kind, tables::Hir2NodeTable};
use ontol_compiler::HirIdx;
use ontol_runtime::{value::PropertyId, DefId, PackageId, RelationId};

#[derive(Debug)]
pub enum LoweringError {
    VariableGap(u32),
}

pub fn lower_hir2(asts: &[Hir2Ast]) -> (Hir2NodeTable<'static>, Vec<HirIdx>) {
    let mut analyzer = AstAnalyzer {
        variables: vec![],
        prop_map: Default::default(),
        relation_counter: 0,
    };
    for ast in asts {
        analyzer.visit_ast(ast);
    }

    let mut table = Hir2NodeTable::default();
    for i in 0..analyzer.variables.len() {
        table.add(Hir2Kind::Variable(i as u32).into());
    }

    let mut builder = TableBuilder {
        table: &mut table,
        prop_map: analyzer.prop_map,
    };

    let root_indices = asts.iter().map(|ast| builder.lower_ast(ast)).collect();

    (table, root_indices)
}

struct TableBuilder<'a> {
    table: &'a mut Hir2NodeTable<'static>,
    prop_map: HashMap<String, PropertyId>,
}

impl<'a> TableBuilder<'a> {
    fn lower_ast(&mut self, ast: &Hir2Ast) -> HirIdx {
        match ast {
            Hir2Ast::VariableRef(var) => {
                self.table.add(Hir2Kind::VariableRef(HirIdx(var.0)).into())
            }
            Hir2Ast::Int(int) => self.table.add(Hir2Kind::Constant(*int).into()),
            Hir2Ast::Unit => self.table.add(Hir2Kind::Unit.into()),
            Hir2Ast::Call(proc, args) => {
                let args = args.iter().map(|arg| self.lower_ast(arg)).collect();
                self.table.add(Hir2Kind::Call(*proc, args).into())
            }
            Hir2Ast::Construct(children) => {
                let children = children.iter().map(|child| self.lower_ast(child)).collect();
                self.table.add(Hir2Kind::Construct(children).into())
            }
            Hir2Ast::ConstructProp(prop, rel, val) => {
                let property_id = self.property_id(prop);
                let rel = self.lower_ast(rel);
                let val = self.lower_ast(val);
                self.table
                    .add(Hir2Kind::ConstructProp(property_id, rel, val).into())
            }
            Hir2Ast::Destruct(var, children) => {
                let var = self.table.add(Hir2Kind::VariableRef(HirIdx(var.0)).into());
                let children = children.iter().map(|child| self.lower_ast(child)).collect();
                self.table.add(Hir2Kind::Destruct(var, children).into())
            }
            Hir2Ast::DestructProp(prop, _arms) => {
                let _property_id = self.property_id(prop);
                todo!()
            }
        }
    }

    fn property_id(&self, prop: &str) -> PropertyId {
        *self.prop_map.get(prop).unwrap()
    }
}

struct AstAnalyzer {
    variables: Vec<bool>,
    prop_map: HashMap<String, PropertyId>,
    relation_counter: u16,
}

impl Hir2AstVisitor for AstAnalyzer {
    fn visit_prop(&mut self, prop: &str) {
        if self.prop_map.contains_key(prop) {
            panic!("Duplicate prop: {prop}");
        }
        let idx = self.relation_counter;
        self.relation_counter += 1;
        let property_id = PropertyId::subject(RelationId(DefId(PackageId(0), idx)));
        self.prop_map.insert(prop.into(), property_id);
    }
    fn visit_binder(&mut self, var: &Hir2AstVariable) {
        self.register(var)
    }
    fn visit_variable(&mut self, var: &Hir2AstVariable) {
        self.register(var)
    }
}

impl AstAnalyzer {
    fn register(&mut self, var: &Hir2AstVariable) {
        let idx = var.0;
        self.variables.resize_with(idx as usize + 1, || false);
        self.variables[idx as usize] = true;
    }
}
