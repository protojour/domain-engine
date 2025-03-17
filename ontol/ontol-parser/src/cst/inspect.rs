//! AST-like APIs on top of the CST

use super::view::{NodeView, NodeViewExt, TokenViewExt, TypedView};
use crate::{
    K,
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

            impl<V: NodeView> TypedView<V> for $kind<V> {
                fn view(&self) -> &V {
                    &self.0
                }

                fn into_view(self) -> V {
                    self.0
                }

                fn from_view(view: V) -> Option<Self> {
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

        $(
            impl<V> From<$kind<V>> for $ident<V> {
                fn from(value: $kind<V>) -> Self {
                    $ident::$kind(value)
                }
            }
        )*

        impl<V: NodeView> TypedView<V> for $ident<V> {
            fn view(&self) -> &V {
                match self {
                    $(
                        Self::$kind($kind(view)) => {
                            view
                        }
                    )*
                }
            }

            fn into_view(self) -> V {
                match self {
                    $(
                        Self::$kind($kind(view)) => {
                            view
                        }
                    )*
                }
            }

            fn from_view(view: V) -> Option<Self> {
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
    SymStatement,
    SymRelation,
    SymDecl,
    ArcStatement,
    ArcClause,
    ArcSlot,
    ArcVar,
    ArcTypeParam,
    RelStatement,
    RelationSet,
    Relation,
    RelSubject,
    RelObject,
    RelParams,
    PropCardinality,
    FmtStatement,
    MapStatement,
    MapArm,
    TypeQuantUnit,
    TypeQuantSet,
    TypeQuantList,
    TypeUnion,
    ThisUnit,
    ThisSet,
    Literal,
    NumberRange,
    RangeStart,
    RangeEnd,
    Uri,
    Name,
    IdentPath,
    Ulid,
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
    SymStatement,
    ArcStatement,
    RelStatement,
    FmtStatement,
    MapStatement,
});

node_union!(TypeQuant {
    TypeQuantUnit,
    TypeQuantSet,
    TypeQuantList,
});

node_union!(TypeRef {
    IdentPath,
    Literal,
    DefBody,
    ThisUnit,
    ThisSet,
    NumberRange,
    TypeUnion,
});

node_union!(ArcItem {
    ArcSlot,
    ArcVar,
    ArcTypeParam,
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

#[derive(Clone)]
pub enum TypeQuantOrPattern<V> {
    TypeQuant(TypeQuant<V>),
    Pattern(Pattern<V>),
}

impl<V: NodeView> Ontol<V> {
    pub fn statements(&self) -> impl Iterator<Item = Statement<V>> {
        self.view().sub_nodes().filter_map(Statement::from_view)
    }
}

impl<V: NodeView> DomainStatement<V> {
    pub fn domain_id(&self) -> Option<Ulid<V>> {
        self.view().sub_nodes().find_map(Ulid::from_view)
    }

    pub fn body(&self) -> Option<DefBody<V>> {
        self.view().sub_nodes().find_map(DefBody::from_view)
    }
}

impl<V: NodeView> UseStatement<V> {
    pub fn uri(&self) -> Option<Uri<V>> {
        self.view().sub_nodes().find_map(Uri::from_view)
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

impl<V: NodeView> SymStatement<V> {
    pub fn sym_relations(&self) -> impl Iterator<Item = SymRelation<V>> {
        self.view().sub_nodes().filter_map(SymRelation::from_view)
    }
}

impl<V: NodeView> SymRelation<V> {
    pub fn decl(&self) -> Option<SymDecl<V>> {
        self.view().sub_nodes().find_map(SymDecl::from_view)
    }
}

impl<V: NodeView> SymDecl<V> {
    pub fn symbol(&self) -> Option<V::Token> {
        self.view().local_tokens_filter(Kind::Symbol).next()
    }
}

impl<V: NodeView> ArcStatement<V> {
    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }

    pub fn arc_clauses(&self) -> impl Iterator<Item = ArcClause<V>> {
        self.view().sub_nodes().filter_map(ArcClause::from_view)
    }
}

impl<V: NodeView> ArcClause<V> {
    pub fn items(&self) -> impl Iterator<Item = ArcItem<V>> {
        self.view().sub_nodes().filter_map(ArcItem::from_view)
    }
}

impl<V: NodeView> ArcVar<V> {
    pub fn symbol(&self) -> Option<V::Token> {
        self.view().local_tokens_filter(Kind::Symbol).next()
    }
}

impl<V: NodeView> ArcTypeParam<V> {
    pub fn ident_path(&self) -> Option<IdentPath<V>> {
        self.view().sub_nodes().find_map(IdentPath::from_view)
    }
}

impl<V: NodeView> ArcSlot<V> {
    pub fn symbol(&self) -> Option<V::Token> {
        self.view().local_tokens_filter(Kind::Symbol).next()
    }
}

impl<V: NodeView> RelStatement<V> {
    pub fn subject(&self) -> Option<RelSubject<V>> {
        self.view().sub_nodes().find_map(RelSubject::from_view)
    }

    pub fn relation_set(&self) -> Option<RelationSet<V>> {
        self.view().sub_nodes().find_map(RelationSet::from_view)
    }

    pub fn object(&self) -> Option<RelObject<V>> {
        self.view().sub_nodes().find_map(RelObject::from_view)
    }
}

impl<V: NodeView> RelSubject<V> {
    pub fn type_quant(&self) -> Option<TypeQuant<V>> {
        self.view().sub_nodes().find_map(TypeQuant::from_view)
    }
}

impl<V: NodeView> RelObject<V> {
    pub fn type_quant_or_pattern(&self) -> Option<TypeQuantOrPattern<V>> {
        self.view().sub_nodes().find_map(|view| {
            if let Some(type_quant) = TypeQuant::from_view(view.clone()) {
                Some(TypeQuantOrPattern::TypeQuant(type_quant))
            } else {
                Pattern::from_view(view).map(TypeQuantOrPattern::Pattern)
            }
        })
    }
}

impl<V: NodeView> RelationSet<V> {
    pub fn relations(&self) -> impl Iterator<Item = Relation<V>> {
        self.view().sub_nodes().filter_map(Relation::from_view)
    }
}

impl<V: NodeView> Relation<V> {
    pub fn relation_type(&self) -> Option<TypeQuant<V>> {
        self.view().sub_nodes().find_map(TypeQuant::from_view)
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
    pub fn transitions(&self) -> impl Iterator<Item = TypeQuant<V>> {
        self.view().sub_nodes().filter_map(TypeQuant::from_view)
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

impl<V: NodeView> TypeQuant<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        match self.clone() {
            Self::TypeQuantUnit(t) => t.type_ref(),
            Self::TypeQuantSet(t) => t.type_ref(),
            Self::TypeQuantList(t) => t.type_ref(),
        }
    }
}

impl<V: NodeView> TypeQuantUnit<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        self.view().sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<V: NodeView> TypeQuantSet<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        self.view().sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<V: NodeView> TypeQuantList<V> {
    pub fn type_ref(&self) -> Option<TypeRef<V>> {
        self.view().sub_nodes().find_map(TypeRef::from_view)
    }
}

impl<V: NodeView> TypeUnion<V> {
    pub fn members(&self) -> impl Iterator<Item = TypeRef<V>> {
        self.view().sub_nodes().filter_map(TypeRef::from_view)
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
    pub fn relation(&self) -> Option<TypeQuant<V>> {
        self.view().sub_nodes().find_map(TypeQuant::from_view)
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
        self.view()
            .local_tokens_filter(|kind| matches!(kind, K![+] | K![-] | K![*] | K![/]))
            .next()
    }
}

impl<V: NodeView> Uri<V> {
    pub fn text(&self) -> Option<UnescapeTextResult> {
        self.view().local_tokens().next()?.literal_text()
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

impl<V: NodeView> Ulid<V> {
    pub fn try_concat_ulid(&self) -> Option<String> {
        const ULID_LEN: usize = 26;

        if self
            .view()
            .local_tokens()
            .fold(0, |sum, token| sum + token.slice().len())
            != ULID_LEN
        {
            return None;
        }

        let mut string = String::with_capacity(ULID_LEN);
        for token in self.view().local_tokens() {
            string.push_str(token.slice());
        }

        Some(string)
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

        assert_eq!(use_stmt.uri().unwrap().text().unwrap().unwrap(), "a");

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
