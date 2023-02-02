use std::{borrow::Cow, collections::HashMap};

use ontol_runtime::{proc::BuiltinProc, DefId, PackageId};
use smallvec::SmallVec;
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
    Tuple(SmallVec<[DefId; 4]>),
    DomainType(String),
    Relation(Relation),
    Relationship(Relationship),
    Property(Property),
    // FIXME: This should not be builtin proc directly.
    // we may find the _actual_ builtin proc to call during type check,
    // if there are different variants per type.
    CoreFn(BuiltinProc),
    Equivalence(Variables, ExprId, ExprId),
}

impl DefKind {
    pub fn opt_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Primitive(Primitive::Int) => Some("int".into()),
            Self::Primitive(Primitive::Number) => Some("number".into()),
            Self::Primitive(Primitive::String) => Some("string".into()),
            Self::StringLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::Tuple(_) => None,
            Self::CoreFn(_) => None,
            Self::DomainType(ident) => Some(ident.as_str().into()),
            Self::Relation(relation) => relation.ident.as_deref().map(|str| str.into()),
            Self::Relationship(_) => None,
            Self::Property(_) => None,
            Self::Equivalence(_, _, _) => None,
        }
    }
}

#[derive(Debug)]
pub struct Variables(pub Box<[ExprId]>);

#[derive(Debug)]
pub enum Primitive {
    /// All the integers
    Int,
    /// All numbers (realistically all rational numbers as all computer numbers are rational)
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
    int: DefId,
    number: DefId,
    string: DefId,
    pub(crate) map: HashMap<DefId, Def>,
    pub(crate) string_literals: HashMap<String, DefId>,
    pub(crate) tuples: HashMap<SmallVec<[DefId; 4]>, DefId>,
}

impl Default for Defs {
    fn default() -> Self {
        let mut defs = Self {
            next_def_id: DefId(0),
            next_expr_id: ExprId(0),
            anonymous_relation: DefId(0),
            int: DefId(0),
            number: DefId(0),
            string: DefId(0),
            map: Default::default(),
            string_literals: Default::default(),
            tuples: Default::default(),
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
        defs.int = defs.add_primitive(Primitive::Int);
        defs.number = defs.add_primitive(Primitive::Number);
        defs.string = defs.add_primitive(Primitive::String);

        defs
    }
}

impl Defs {
    pub fn anonymous_relation(&self) -> DefId {
        self.anonymous_relation
    }

    pub fn int(&self) -> DefId {
        self.int
    }

    pub fn number(&self) -> DefId {
        self.number
    }

    pub fn string(&self) -> DefId {
        self.string
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

    pub fn def_tuple(&mut self, elements: SmallVec<[DefId; 4]>) -> DefId {
        match self.tuples.get(elements.as_slice()) {
            Some(def_id) => *def_id,
            None => {
                let def_id = self.add_def(
                    DefKind::Tuple(elements.clone()),
                    CORE_PKG,
                    SourceSpan::none(),
                );
                self.tuples.insert(elements, def_id);
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

    fn add_primitive(&mut self, primitive: Primitive) -> DefId {
        self.add_def(DefKind::Primitive(primitive), CORE_PKG, SourceSpan::none())
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

        // fundamental types
        let int_ty = self.types.intern(Type::Int(self.defs.int));
        let num_ty = self.types.intern(Type::Number(self.defs.number));
        let string_ty = self.types.intern(Type::String(self.defs.string));

        // add fundamental types to core namespace
        let int = self.def_core_type("int", self.defs.int, int_ty);
        let number = self.def_core_type("number", self.defs.number, num_ty);
        let string = self.def_core_type("string", self.defs.string, string_ty);

        let int_int = self.types.intern([int, int]);
        let string_string = self.types.intern([string, string]);

        let int_int_to_int = self.types.intern(Type::Function {
            params: int_int,
            output: int,
        });
        let string_string_to_string = self.types.intern(Type::Function {
            params: string_string,
            output: string,
        });

        // Built-in functions
        // arithmetic
        self.def_core_proc("+", DefKind::CoreFn(BuiltinProc::Add), int_int_to_int);
        self.def_core_proc("-", DefKind::CoreFn(BuiltinProc::Sub), int_int_to_int);
        self.def_core_proc("*", DefKind::CoreFn(BuiltinProc::Mul), int_int_to_int);
        self.def_core_proc("/", DefKind::CoreFn(BuiltinProc::Div), int_int_to_int);

        // string manipulation
        self.def_core_proc(
            "append",
            DefKind::CoreFn(BuiltinProc::Append),
            string_string_to_string,
        );

        self
    }

    fn def_core_type(&mut self, ident: &str, def_id: DefId, ty: TypeRef<'m>) -> TypeRef<'m> {
        self.namespaces
            .get_mut(CORE_PKG, Space::Type)
            .insert(ident.into(), def_id);
        self.def_types.map.insert(def_id, ty);
        ty
    }

    fn def_core_proc(&mut self, ident: &str, def_kind: DefKind, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(ident, Space::Type, def_kind, CORE_PKG, SourceSpan::none());
        self.def_types.map.insert(def_id, ty);

        def_id
    }
}
