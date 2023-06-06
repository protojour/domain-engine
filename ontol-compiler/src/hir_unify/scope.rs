use ontol_hir::kind::NodeKind;

use crate::typed_hir::TypedHirNode;

pub fn hir_subscope<'s, 'm>(hir: &'s TypedHirNode<'m>, index: usize) -> &'s TypedHirNode<'m> {
    match &hir.kind {
        NodeKind::VariableRef(..) | NodeKind::Unit | NodeKind::Int(_) => panic!(),
        NodeKind::Let(_, definition, body) => {
            if index == 0 {
                definition
            } else {
                &body[index - 1]
            }
        }
        NodeKind::Call(_, sub_scopes) => &sub_scopes[index],
        NodeKind::Map(sub_scope) => sub_scope,
        NodeKind::Seq(_, attr) => &attr[index],
        NodeKind::Struct(_, sub_scopes) => &sub_scopes[index],
        NodeKind::Prop(..) => {
            panic!("prop-variant subscope")
        }
        NodeKind::MatchProp(..) => {
            panic!("match-prop is not a scope")
        }
        NodeKind::Gen(_, _, sub_scopes) => &sub_scopes[index],
        NodeKind::Iter(_, _, sub_scopes) => &sub_scopes[index],
        NodeKind::Push(_, attr) => &attr[index],
    }
}
