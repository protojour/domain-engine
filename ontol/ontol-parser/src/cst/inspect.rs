//! AST-like APIs on top of the CST

use super::view::{NodeView, NodeViewExt, TokenViewExt};
use crate::lexer::{kind::Kind, unescape::UnescapeTextResult};

/// Does two things:
/// 1. Create `enum Node` that has all the node kinds.
/// 2. Create a tuple struct for each node kind.
///
/// The data for each of the enum variants is that tuple struct.
///
/// Example:
/// ```
/// enum Node<V> {
///     PatStruct(PatStruct<V>)
/// }
///
/// struct PatStruct<V> {
///     pub node: V,
/// };
/// ```
macro_rules! nodes {
    ($ident:ident { $($kind:ident),* $(,)? }) => {
        node_union!($ident { $($kind),*, });

        $(
            #[derive(Clone, Copy)]
            pub struct $kind<V> {
                pub view: V,
            }

            impl<'a, V: NodeView<'a>> $kind<V> {
                pub fn from_view(view: V) -> Option<Self> {
                    if view.kind() == Kind::$kind {
                        Some(Self { view })
                    } else {
                        None
                    }
                }
            }
        )*
    };
}

/// Create a node enum of the given ident.
/// The variants must be valid nodes.
///
/// ```
/// enum MyUnion<V> {
///     PatStruct(PatStruct<V>),
/// }
/// ```
macro_rules! node_union {
    ($ident:ident { $($kind:ident),* $(,)? }) => {
        pub enum $ident<V> {
            $($kind($kind<V>)),*
        }

        $(
            impl<V> From<$kind<V>> for $ident<V> {
                fn from(value: $kind<V>) -> Self {
                    $ident::$kind(value)
                }
            }
        )*

        impl<'a, V: NodeView<'a>> $ident<V> {
            pub(crate) fn from_view(view: V) -> Option<Self> {
                match view.kind() {
                    $(
                        Kind::$kind => Some(Self::$kind($kind { view }))
                    ),*,
                    _ => None
                }
            }
        }
    };
}

nodes!(Node {
    Error,
    Ontol,
    UseStatement,
    DefStatement,
    DefBody,
    RelStatement,
    RelFwdSet,
    RelBackwdSet,
    Relation,
    RelSubject,
    RelObject,
    RelParams,
    PropCardinality,
    FmtStatement,
    MapStatement,
    MapArm,
    TypeModUnit,
    TypeModSet,
    TypeModSeq,
    This,
    Literal,
    Range,
    Location,
    IdentPath,
    PatStruct,
    PatSet,
    PatAtom,
    PatBinary,
    StructParamAttrProp,
    StructParamAttrUnit,
    StructAttrRelArgs,
    SetElement,
    Spread,
});

node_union!(Statement {
    UseStatement,
    DefStatement,
    RelStatement,
    FmtStatement,
    MapStatement,
});

node_union!(TypeMod {
    TypeModUnit,
    TypeModSet,
    TypeModSeq,
});

node_union!(TypeRef {
    IdentPath,
    DefBody,
    This,
});

/*
node_enum!(Pattern {
    PatStruct,
    PatSet,
    PatAtom,
    PatBinary,
});

node_enum!(StructParamAttr {
    StructParamAttrProp,
    StructParamAttrUnit,
});
*/

impl<'a, V: NodeView<'a> + 'a> Ontol<V> {
    pub fn statements(self) -> impl Iterator<Item = Statement<V>> + 'a {
        self.view.sub_nodes().filter_map(Statement::from_view)
    }
}

impl<'a, V: NodeView<'a>> UseStatement<V> {
    pub fn location(self) -> Option<Location<V>> {
        self.view.sub_nodes().find_map(Location::from_view)
    }

    pub fn ident_path(self) -> Option<IdentPath<V>> {
        self.view.sub_nodes().find_map(IdentPath::from_view)
    }
}

impl<'a, V: NodeView<'a>> DefStatement<V> {
    pub fn doc_comments(self) -> impl Iterator<Item = &'a str> {
        self.view.local_doc_comments()
    }

    pub fn modifiers(self) -> impl Iterator<Item = V::Token> {
        self.view.local_tokens_filter(Kind::Modifier)
    }

    pub fn ident_path(self) -> Option<IdentPath<V>> {
        self.view.sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn body(self) -> Option<DefBody<V>> {
        self.view.sub_nodes().find_map(DefBody::from_view)
    }
}

impl<'a, V: NodeView<'a> + 'a> DefBody<V> {
    pub fn statements(self) -> impl Iterator<Item = Statement<V>> + 'a {
        self.view.sub_nodes().filter_map(Statement::from_view)
    }
}

impl<'a, V: NodeView<'a>> RelStatement<V> {
    pub fn doc_comments(self) -> impl Iterator<Item = &'a str> {
        self.view.local_doc_comments()
    }

    pub fn subject(self) -> Option<RelSubject<V>> {
        self.view.sub_nodes().find_map(RelSubject::from_view)
    }

    pub fn fwd_set(self) -> Option<RelFwdSet<V>> {
        self.view.sub_nodes().find_map(RelFwdSet::from_view)
    }

    pub fn backwd_set(self) -> Option<RelBackwdSet<V>> {
        self.view.sub_nodes().find_map(RelBackwdSet::from_view)
    }

    pub fn object(self) -> Option<RelObject<V>> {
        self.view.sub_nodes().find_map(RelObject::from_view)
    }
}

impl<'a, V: NodeView<'a>> RelSubject<V> {
    pub fn type_mod(self) -> Option<TypeMod<V>> {
        self.view.sub_nodes().find_map(TypeMod::from_view)
    }
}

impl<'a, V: NodeView<'a>> RelObject<V> {
    pub fn type_mod(self) -> Option<TypeMod<V>> {
        self.view.sub_nodes().find_map(TypeMod::from_view)
    }
}

impl<'a, V: NodeView<'a>> FmtStatement<V> {}

impl<'a, V: NodeView<'a>> MapStatement<V> {
    pub fn doc_comments(self) -> impl Iterator<Item = &'a str> {
        self.view.local_doc_comments()
    }
}

impl<'a, V: NodeView<'a>> TypeMod<V> {
    pub fn type_ref(self) -> Option<TypeRef<V>> {
        match self {
            Self::TypeModUnit(t) => t.type_ref(),
            Self::TypeModSet(t) => t.type_ref(),
            Self::TypeModSeq(t) => t.type_ref(),
        }
    }
}

impl<'a, V: NodeView<'a>> TypeModUnit<V> {
    pub fn type_ref(self) -> Option<TypeRef<V>> {
        self.view.sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<'a, V: NodeView<'a>> TypeModSet<V> {
    pub fn type_ref(self) -> Option<TypeRef<V>> {
        self.view.sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<'a, V: NodeView<'a>> TypeModSeq<V> {
    pub fn type_ref(self) -> Option<TypeRef<V>> {
        self.view.sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<'a, V: NodeView<'a>> Location<V> {
    pub fn text(self) -> Option<UnescapeTextResult> {
        self.view.local_tokens().next()?.literal_text()
    }
}

impl<'a, V: NodeView<'a>> IdentPath<V> {
    pub fn symbols(self) -> impl Iterator<Item = V::Token> {
        self.view.local_tokens_filter(Kind::Sym)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cst::{
            grammar,
            parser::CstParser,
            view::{NodeViewExt, TokenView},
        },
        lexer::cst_lex,
    };

    use super::{Node, Statement};

    #[test]
    fn iter_statements() {
        let src = "use 'a' as b";
        let (lex, _) = cst_lex(src);
        let mut parser = CstParser::from_lexed_source(src, lex);
        grammar::ontol(&mut parser);
        let (flat_tree, _) = parser.finish();
        let tree = flat_tree.unflatten();

        let Node::Ontol(ontol) = tree.view(src).node() else {
            panic!()
        };

        let statements: Vec<_> = ontol.statements().collect();
        assert_eq!(statements.len(), 1);

        let Statement::UseStatement(use_stmt) = statements.into_iter().next().unwrap() else {
            panic!()
        };

        assert_eq!(use_stmt.location().unwrap().text().unwrap().unwrap(), "a");

        assert_eq!(
            use_stmt
                .ident_path()
                .unwrap()
                .symbols()
                .next()
                .unwrap()
                .slice(),
            "b"
        );
    }
}
