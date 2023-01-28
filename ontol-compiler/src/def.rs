use std::collections::HashMap;

use ontol_runtime::{DefId, PackageId};
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    expr::ExprId,
    mem::Intern,
    namespace::Space,
    relation::Role,
    source::{Package, SourceSpan, CORE_PKG},
    types::Type,
};

/// A definition in some package
#[derive(Debug)]
pub struct Def {
    pub package: PackageId,
    pub kind: DefKind,
    pub span: SourceSpan,
}

#[derive(Debug)]
pub enum DefKind {
    Primitive(Primitive),
    CoreFn(CoreFn),
    Type(String),
    Relation(Relation),
    Relationship(Relationship),
    Property(Property),
    AnonField { type_def_id: DefId },
    NamedField { ident: String, type_def_id: DefId },
    Equivalence(ExprId, ExprId),
}

#[derive(Debug)]
pub enum Primitive {
    Number,
    String,
}

#[derive(Debug)]
pub enum CoreFn {
    Mul,
    Div,
}

/// This definition expresses that a relation _exists_
#[derive(Debug)]
pub struct Relation {
    pub ident: Option<String>,
    pub subject_prop: Option<String>,
    pub object_prop: Option<String>,
}

/// This definition expresses that a relation is a relationship between a subject and an object
#[derive(Debug)]
pub struct Relationship {
    pub relation_def_id: DefId,
    pub subject: DefId,
    pub object: DefId,
}

#[derive(Debug)]
pub struct Property {
    pub relation_def_id: DefId,
    pub role: Role,
}

impl Relation {
    pub fn subject_prop(&self) -> Option<&String> {
        self.subject_prop.as_ref().or(self.ident.as_ref())
    }

    pub fn object_prop(&self) -> Option<&String> {
        self.object_prop.as_ref().or(self.ident.as_ref())
    }
}

#[derive(Debug)]
pub struct Defs {
    next_def_id: DefId,
    next_expr_id: ExprId,
    pub(crate) map: HashMap<DefId, Def>,
}

impl Default for Defs {
    fn default() -> Self {
        Self {
            next_def_id: DefId(0),
            next_expr_id: ExprId(0),
            map: Default::default(),
        }
    }
}

impl Defs {
    pub fn get_def_kind(&self, def_id: DefId) -> Option<&DefKind> {
        self.map.get(&def_id).map(|def| &def.kind)
    }

    pub fn get_relationship_defs(
        &self,
        relationship_def_id: DefId,
    ) -> Result<(&Relationship, &Relation), ()> {
        let DefKind::Relationship(relationship) = self.get_def_kind(relationship_def_id).ok_or(())? else {
            return Err(());
        };
        let DefKind::Relation(relation) = self.get_def_kind(relationship.relation_def_id).ok_or(())? else {
            return Err(());
        };
        Ok((relationship, relation))
    }

    pub fn alloc_def_id(&mut self) -> DefId {
        let id = self.next_def_id;
        self.next_def_id.0 += 1;
        id
    }

    pub fn alloc_expr_id(&mut self) -> ExprId {
        let id = self.next_expr_id;
        self.next_expr_id.0 += 1;
        id
    }

    pub fn add_def(&mut self, kind: DefKind, package: PackageId, span: SourceSpan) -> DefId {
        let def_id = self.alloc_def_id();
        self.map.insert(
            def_id,
            Def {
                package,
                span,
                kind,
            },
        );

        def_id
    }
}

impl<'m> Compiler<'m> {
    pub fn add_named_def(
        &mut self,
        name: &str,
        space: Space,
        kind: DefKind,
        package: PackageId,
        span: SourceSpan,
    ) -> DefId {
        let def_id = self.defs.alloc_def_id();
        self.namespaces
            .get_mut(package, space)
            .insert(name.into(), def_id);
        self.defs.map.insert(
            def_id,
            Def {
                package,
                span,
                kind,
            },
        );

        def_id
    }

    pub fn with_core(mut self) -> Self {
        self.packages.insert(
            CORE_PKG,
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
            "string",
            DefKind::Primitive(Primitive::String),
            Type::String,
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
        let def_id = self.add_named_def(name, Space::Type, def_kind, CORE_PKG, SourceSpan::none());
        self.def_types
            .map
            .insert(def_id, self.types.intern(type_kind));

        def_id
    }
}
