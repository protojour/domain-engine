use std::collections::HashMap;

use crate::{
    env::Env,
    expr::ExprId,
    mem::Intern,
    misc::{Package, PackageId},
    types::Type,
    SString,
};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct DefId(pub u32);

/// A definition in some package
pub struct Def {
    pub package: PackageId,
    pub kind: DefKind,
}

#[derive(Debug)]
pub enum DefKind {
    Primitive(Primitive),
    CoreFn(CoreFn),
    Constructor(SString, DefId),
    Field { type_def_id: DefId },
    Equivalence(ExprId, ExprId),
}

#[derive(Debug)]
pub enum Primitive {
    Number,
}

#[derive(Debug)]
pub enum CoreFn {
    Mul,
    Div,
}

#[derive(Default)]
pub struct Namespaces {
    namespaces: HashMap<PackageId, HashMap<SString, DefId>>,
}

impl Namespaces {
    pub fn lookup(&self, search_path: &[PackageId], ident: &str) -> Option<DefId> {
        for package in search_path {
            let Some(namespace) = self.namespaces.get(package) else {
                continue
            };
            if let Some(def_id) = namespace.get(ident) {
                return Some(*def_id);
            };
        }

        None
    }

    pub fn get_mut(&mut self, package: PackageId) -> &mut HashMap<SString, DefId> {
        self.namespaces.entry(package).or_default()
    }
}

impl<'m> Env<'m> {
    pub fn add_def(&mut self, package: PackageId, kind: DefKind) -> DefId {
        let def_id = self.alloc_def_id();
        self.defs.insert(def_id, Def { package, kind });

        def_id
    }

    pub fn add_named_def(&mut self, package: PackageId, name: &str, kind: DefKind) -> DefId {
        let def_id = self.alloc_def_id();
        self.namespaces.get_mut(package).insert(name.into(), def_id);
        self.defs.insert(def_id, Def { package, kind });

        def_id
    }

    pub fn with_core(mut self) -> Self {
        let core = PackageId(0);
        self.packages.insert(
            core,
            Package {
                name: "core".into(),
            },
        );

        let num = self.types.intern(Type::Number);
        let num_num = self.types.intern([num, num]);

        self.add_core_def(
            "number",
            DefKind::Primitive(Primitive::Number),
            Type::Number,
        );
        self.add_core_def(
            "*",
            DefKind::CoreFn(CoreFn::Mul),
            Type::Function {
                params: num_num,
                output: num,
            },
        );
        self.add_core_def(
            "/",
            DefKind::CoreFn(CoreFn::Div),
            Type::Function {
                params: num_num,
                output: num,
            },
        );
        self
    }

    fn add_core_def(&mut self, name: &str, def_kind: DefKind, type_kind: Type<'m>) -> DefId {
        let def_id = self.add_named_def(PackageId(0), name, def_kind);
        self.def_types.insert(def_id, self.types.intern(type_kind));

        def_id
    }
}
