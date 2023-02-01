use std::{borrow::Cow, collections::HashMap};

use ontol_runtime::{proc::BuiltinProc, DefId, PackageId};
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    expr::ExprId,
    mem::Intern,
    namespace::Space,
    relation::Role,
    source::{Package, SourceSpan, CORE_PKG},
    types::{Type, TypeRef},
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
    StringLiteral(String),
    CoreFn(BuiltinProc),
    Type(String),
    Relation(Relation),
    Relationship(Relationship),
    Property(Property),
    Equivalence(Variables, ExprId, ExprId),
}

impl DefKind {
    pub fn diagnostics_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Primitive(Primitive::Number) => Some("number".into()),
            Self::Primitive(Primitive::String) => Some("string".into()),
            Self::StringLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::CoreFn(_) => None,
            Self::Type(ident) => Some(ident.as_str().into()),
            Self::Relation(relation) => relation.ident.as_deref().map(|str| str.into()),
            Self::Relationship(_) => None,
            Self::Property(_) => None,
            Self::Equivalence(_, _, _) => Some("eq!".into()),
        }
    }
}

#[derive(Debug)]
pub struct Variables(pub Box<[ExprId]>);

#[derive(Debug)]
pub enum Primitive {
    Number,
    String,
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
    anonymous_relation: DefId,
    pub(crate) map: HashMap<DefId, Def>,
    pub(crate) string_literals: HashMap<String, DefId>,
}

impl Default for Defs {
    fn default() -> Self {
        let mut defs = Self {
            next_def_id: DefId(0),
            next_expr_id: ExprId(0),
            anonymous_relation: DefId(0),
            map: Default::default(),
            string_literals: Default::default(),
        };

        // Add some extremely fundamental definitions here already.
        // These are even independent from CORE being defined.

        // The anonymous / "manifested-as" relation
        defs.anonymous_relation = defs.add_def(
            DefKind::Relation(Relation {
                ident: None,
                subject_prop: None,
                object_prop: None,
            }),
            CORE_PKG,
            SourceSpan::none(),
        );

        defs
    }
}

impl Defs {
    pub fn anonymous_relation(&self) -> DefId {
        self.anonymous_relation
    }

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

    pub fn def_string_literal(&mut self, lit: String) -> DefId {
        match self.string_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let def_id = self.add_def(
                    DefKind::StringLiteral(lit.clone()),
                    CORE_PKG,
                    SourceSpan::none(),
                );
                self.string_literals.insert(lit, def_id);
                def_id
            }
        }
    }

    pub fn get_string_literal(&self, def_id: DefId) -> &str {
        match self.get_def_kind(def_id) {
            Some(DefKind::StringLiteral(lit)) => &lit,
            kind => panic!("BUG: not a string literal: {kind:?}"),
        }
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
        let str = self.types.intern(Type::String);
        let str_str = self.types.intern([str, str]);

        let num_num_to_num = self.types.intern(Type::Function {
            params: num_num,
            output: num,
        });
        let str_str_to_str = self.types.intern(Type::Function {
            params: str_str,
            output: str,
        });

        // Built-in types
        self.add_core_def("number", DefKind::Primitive(Primitive::Number), num);
        self.add_core_def("string", DefKind::Primitive(Primitive::String), str);

        // Built-in functions
        // arithmetic
        self.add_core_def("+", DefKind::CoreFn(BuiltinProc::Add), num_num_to_num);
        self.add_core_def("-", DefKind::CoreFn(BuiltinProc::Sub), num_num_to_num);
        self.add_core_def("*", DefKind::CoreFn(BuiltinProc::Mul), num_num_to_num);
        self.add_core_def("/", DefKind::CoreFn(BuiltinProc::Div), num_num_to_num);

        // string manipulation
        self.add_core_def(
            "append",
            DefKind::CoreFn(BuiltinProc::Append),
            str_str_to_str,
        );

        self
    }

    fn add_core_def(&mut self, name: &str, def_kind: DefKind, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(name, Space::Type, def_kind, CORE_PKG, SourceSpan::none());
        self.def_types.map.insert(def_id, ty);

        def_id
    }
}
