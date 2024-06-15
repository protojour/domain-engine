//! AST-like APIs on top of the CST

use super::view::{NodeView, NodeViewExt, TokenViewExt};
use crate::{
    cst::view::TokenView,
    lexer::{kind::Kind, unescape::UnescapeTextResult},
};

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
        $(
            #[derive(Clone, Copy)]
            pub struct $kind<V>(pub V);

            impl<V: NodeView> $kind<V> {
                pub fn view(&self) -> V {
                    self.0.clone()
                }

                pub fn from_view(view: V) -> Option<Self> {
                    if view.kind() == Kind::$kind {
                        Some(Self(view))
                    } else {
                        None
                    }
                }
            }
        )*

        node_union!($ident { $($kind),*, });
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
        #[derive(Clone, Copy)]
        pub enum $ident<V> {
            $($kind($kind<V>)),*
        }

        impl<V: NodeView> $ident<V> {
            pub fn view(&self) -> V {
                match self {
                    $(
                        Self::$kind($kind(view)) => {
                            view.clone()
                        }
                    )*
                }
            }
        }

        $(
            impl<V> From<$kind<V>> for $ident<V> {
                fn from(value: $kind<V>) -> Self {
                    $ident::$kind(value)
                }
            }
        )*

        impl<V: NodeView> $ident<V> {
            #[allow(unused)]
            pub(crate) fn from_view(view: V) -> Option<Self> {
                match view.kind() {
                    $(
                        Kind::$kind => Some(Self::$kind($kind(view)))
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
    DomainStatement,
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
    TypeModList,
    This,
    Literal,
    NumberRange,
    RangeStart,
    RangeEnd,
    Name,
    IdentPath,
    PatStruct,
    PatSet,
    PatAtom,
    PatBinary,
    StructParamAttrProp,
    StructParamAttrUnit,
    RelArgs,
    SetElement,
    Spread,
});

node_union!(Statement {
    DomainStatement,
    UseStatement,
    DefStatement,
    RelStatement,
    FmtStatement,
    MapStatement,
});

node_union!(TypeMod {
    TypeModUnit,
    TypeModSet,
    TypeModList,
});

node_union!(TypeRef {
    IdentPath,
    Literal,
    DefBody,
    This,
    NumberRange,
});

node_union!(Pattern {
    PatStruct,
    PatSet,
    PatAtom,
    PatBinary,
});

node_union!(StructParam {
    StructParamAttrProp,
    Spread,
    StructParamAttrUnit,
});

pub enum TypeModOrPattern<V> {
    TypeMod(TypeMod<V>),
    Pattern(Pattern<V>),
}

impl<V: NodeView> Ontol<V> {
    pub fn statements(&self) -> impl Iterator<Item = Statement<V>> {
        self.view().sub_nodes().filter_map(Statement::from_view)
    }
}

impl<V: NodeView> DomainStatement<V> {
    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn body(&self) -> Option<DefBody<V>> {
        self.view().sub_nodes().find_map(DefBody::from_view)
    }
}

impl<V: NodeView> UseStatement<V> {
    pub fn name(&self) -> Option<Name<V>> {
        self.view().sub_nodes().find_map(Name::from_view)
    }

    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }
}

impl<V: NodeView> DefStatement<V> {
    pub fn modifiers(&self) -> impl Iterator<Item = V::Token> {
        self.view().local_tokens_filter(Kind::Modifier)
    }

    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn body(&self) -> Option<DefBody<V>> {
        self.view().sub_nodes().find_map(DefBody::from_view)
    }
}

impl<V: NodeView> DefBody<V> {
    pub fn statements(&self) -> impl Iterator<Item = Statement<V>> {
        self.view().sub_nodes().filter_map(Statement::from_view)
    }
}

impl<V: NodeView> RelStatement<V> {
    pub fn subject(&self) -> Option<RelSubject<V>> {
        self.view().sub_nodes().find_map(RelSubject::from_view)
    }

    pub fn fwd_set(&self) -> Option<RelFwdSet<V>> {
        self.view().sub_nodes().find_map(RelFwdSet::from_view)
    }

    pub fn backwd_set(&self) -> Option<RelBackwdSet<V>> {
        self.view().sub_nodes().find_map(RelBackwdSet::from_view)
    }

    pub fn object(&self) -> Option<RelObject<V>> {
        self.view().sub_nodes().find_map(RelObject::from_view)
    }
}

impl<V: NodeView> RelSubject<V> {
    pub fn type_mod(&self) -> Option<TypeMod<V>> {
        self.view().sub_nodes().find_map(TypeMod::from_view)
    }
}

impl<V: NodeView> RelObject<V> {
    pub fn type_mod_or_pattern(&self) -> Option<TypeModOrPattern<V>> {
        self.view().sub_nodes().find_map(|view| {
            if let Some(type_mod) = TypeMod::from_view(view.clone()) {
                Some(TypeModOrPattern::TypeMod(type_mod))
            } else {
                Pattern::from_view(view).map(TypeModOrPattern::Pattern)
            }
        })
    }
}

impl<V: NodeView> RelFwdSet<V> {
    pub fn relations(&self) -> impl Iterator<Item = Relation<V>> {
        self.view().sub_nodes().filter_map(Relation::from_view)
    }
}

/// TODO: In the AST parser, the backward relation set can contain
/// many things, but it's never used in examples, so it's not implemented here yet
impl<V: NodeView> RelBackwdSet<V> {
    pub fn name(&self) -> Option<Name<V>> {
        self.view().sub_nodes().find_map(Name::from_view)
    }

    pub fn prop_cardinality(&self) -> Option<PropCardinality<V>> {
        self.view().sub_nodes().find_map(PropCardinality::from_view)
    }
}

impl<V: NodeView> Relation<V> {
    pub fn relation_type(&self) -> Option<TypeMod<V>> {
        self.view().sub_nodes().find_map(TypeMod::from_view)
    }

    pub fn rel_params(&self) -> Option<RelParams<V>> {
        self.view().sub_nodes().find_map(RelParams::from_view)
    }

    pub fn prop_cardinality(&self) -> Option<PropCardinality<V>> {
        self.view().sub_nodes().find_map(PropCardinality::from_view)
    }
}

impl<V: NodeView> RelParams<V> {
    pub fn statements(&self) -> impl Iterator<Item = Statement<V>> {
        self.view().sub_nodes().filter_map(Statement::from_view)
    }
}

impl<V: NodeView> PropCardinality<V> {
    pub fn question(&self) -> Option<V::Token> {
        self.view().local_tokens_filter(Kind::Question).next()
    }
}

impl<V: NodeView> FmtStatement<V> {
    pub fn transitions(&self) -> impl Iterator<Item = TypeMod<V>> {
        self.view().sub_nodes().filter_map(TypeMod::from_view)
    }
}

impl<V: NodeView> MapStatement<V> {
    pub fn modifiers(&self) -> impl Iterator<Item = V::Token> {
        self.view().local_tokens_filter(Kind::Modifier)
    }

    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn arms(&self) -> impl Iterator<Item = MapArm<V>> {
        self.view().sub_nodes().filter_map(MapArm::from_view)
    }
}

impl<V: NodeView> MapArm<V> {
    pub fn pattern(&self) -> Option<Pattern<V>> {
        self.view().sub_nodes().find_map(Pattern::from_view)
    }
}

impl<V: NodeView> TypeMod<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        match self.clone() {
            Self::TypeModUnit(t) => t.type_ref(),
            Self::TypeModSet(t) => t.type_ref(),
            Self::TypeModList(t) => t.type_ref(),
        }
    }
}

impl<V: NodeView> TypeModUnit<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        self.view().sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<V: NodeView> TypeModSet<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        self.view().sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<V: NodeView> TypeModList<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        self.view().sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<V: NodeView> PatStruct<V> {
    pub fn modifiers(&self) -> impl Iterator<Item = V::Token> {
        self.view().local_tokens_filter(Kind::Modifier)
    }

    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn params(&self) -> impl Iterator<Item = StructParam<V>> {
        self.view().sub_nodes().filter_map(StructParam::from_view)
    }
}

impl<V: NodeView> StructParamAttrProp<V> {
    pub fn relation(&self) -> Option<TypeMod<V>> {
        self.view().sub_nodes().find_map(TypeMod::from_view)
    }

    pub fn rel_args(&self) -> Option<RelArgs<V>> {
        self.view().sub_nodes().find_map(RelArgs::from_view)
    }

    pub fn prop_cardinality(&self) -> Option<PropCardinality<V>> {
        self.view().sub_nodes().find_map(PropCardinality::from_view)
    }

    pub fn pattern(&self) -> Option<Pattern<V>> {
        self.view().sub_nodes().find_map(Pattern::from_view)
    }
}

impl<V: NodeView> RelArgs<V> {
    pub fn params(&self) -> impl Iterator<Item = StructParam<V>> {
        self.view().sub_nodes().filter_map(StructParam::from_view)
    }
}

impl<V: NodeView> StructParamAttrUnit<V> {
    pub fn pattern(&self) -> Option<Pattern<V>> {
        self.view().sub_nodes().find_map(Pattern::from_view)
    }
}

impl<V: NodeView> PatSet<V> {
    pub fn modifier(&self) -> Option<V::Token> {
        self.view().local_tokens_filter(Kind::Modifier).next()
    }

    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn elements(&self) -> impl Iterator<Item = SetElement<V>> {
        self.view().sub_nodes().filter_map(SetElement::from_view)
    }
}

impl<V: NodeView> SetElement<V> {
    pub fn spread(&self) -> Option<Spread<V>> {
        self.view().sub_nodes().find_map(Spread::from_view)
    }

    pub fn rel_args(&self) -> Option<RelArgs<V>> {
        self.view().sub_nodes().find_map(RelArgs::from_view)
    }

    pub fn pattern(&self) -> Option<Pattern<V>> {
        self.view().sub_nodes().find_map(Pattern::from_view)
    }
}

impl<V: NodeView> PatBinary<V> {
    pub fn operands(&self) -> impl Iterator<Item = Pattern<V>> {
        self.view().sub_nodes().filter_map(Pattern::from_view)
    }

    pub fn infix_token(&self) -> Option<V::Token> {
        self.view().local_tokens().find(|token| {
            matches!(
                token.kind(),
                Kind::Plus | Kind::Minus | Kind::Star | Kind::Div
            )
        })
    }
}

impl<V: NodeView> Name<V> {
    pub fn text(&self) -> Option<UnescapeTextResult> {
        self.view().local_tokens().next()?.literal_text()
    }
}

impl<V: NodeView> IdentPath<V> {
    pub fn symbols(&self) -> impl Iterator<Item = V::Token> {
        self.view().local_tokens_filter(Kind::Symbol)
    }
}

impl<V: NodeView> NumberRange<V> {
    pub fn start(&self) -> Option<V::Token> {
        let start = self
            .view()
            .sub_nodes()
            .find(|view| view.kind() == Kind::RangeStart)?;
        start.local_tokens_filter(Kind::Number).next()
    }

    pub fn end(&self) -> Option<V::Token> {
        let end = self
            .view()
            .sub_nodes()
            .find(|view| view.kind() == Kind::RangeEnd)?;
        end.local_tokens_filter(Kind::Number).next()
    }
}

impl<V: NodeView> Spread<V> {
    pub fn symbol(&self) -> Option<V::Token> {
        self.view().local_tokens_filter(Kind::Symbol).next()
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

        assert_eq!(use_stmt.name().unwrap().text().unwrap().unwrap(), "a");

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
