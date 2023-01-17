use crate::{
    env::{Env, Intern},
    misc::{Package, PackageId},
    types::TypeKind,
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
    Constructor(String, DefId),
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

impl<'m> Env<'m> {
    pub fn def_by_name(&self, package: PackageId, name: &str) -> Option<DefId> {
        Some(*self.namespace.borrow().get(&package)?.get(name)?)
    }

    pub fn core_def_by_name(&self, name: &str) -> Option<DefId> {
        self.def_by_name(PackageId(0), name)
    }

    pub fn add_def(&mut self, package: PackageId, name: &str, kind: DefKind) -> DefId {
        let def_id = DefId(
            self.def_counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );
        let mut namespace = self.namespace.borrow_mut();
        namespace
            .entry(package)
            .or_insert_with(|| Default::default())
            .insert(name.into(), def_id);
        let mut defs = self.defs.borrow_mut();
        defs.insert(def_id, Def { package, kind });

        def_id
    }

    pub fn register_builtins(&mut self) {
        let core = PackageId(0);
        self.packages.insert(
            core,
            Package {
                name: "core".into(),
            },
        );

        let num = self.intern(TypeKind::Number);
        let num_num = self.intern([num, num]);

        self.add_core_def(
            "Number",
            DefKind::Primitive(Primitive::Number),
            TypeKind::Number,
        );
        self.add_core_def(
            "*",
            DefKind::CoreFn(CoreFn::Mul),
            TypeKind::Function {
                args: num_num,
                output: num,
            },
        );
        self.add_core_def(
            "/",
            DefKind::CoreFn(CoreFn::Div),
            TypeKind::Function {
                args: num_num,
                output: num,
            },
        );
    }

    fn add_core_def(&mut self, name: &str, def_kind: DefKind, type_kind: TypeKind<'m>) -> DefId {
        let def_id = self.add_def(PackageId(0), name, def_kind);
        self.def_types
            .borrow_mut()
            .insert(def_id, self.intern(type_kind));

        def_id
    }
}
