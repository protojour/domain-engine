use bit_set::BitSet;
use fnv::FnvHashMap;
use ontos::kind::{NodeKind, Variable};
use smallvec::SmallVec;

use crate::typed_ontos::lang::OntosNode;

#[derive(Clone, Default, Debug)]
pub struct Path(pub SmallVec<[u16; 32]>);

pub struct LocateCtx<'a> {
    variables: &'a BitSet,
    output: FnvHashMap<Variable, Path>,
    current_path: Path,
}

impl<'a> LocateCtx<'a> {
    fn enter_child(&mut self, index: usize, func: impl Fn(&mut Self)) {
        self.current_path.0.push(index as u16);
        func(self);
        self.current_path.0.pop();
    }
}

pub fn locate_variables(node: &OntosNode, variables: &BitSet) -> FnvHashMap<Variable, Path> {
    let mut ctx = LocateCtx {
        variables,
        output: FnvHashMap::default(),
        current_path: Path::default(),
    };
    locate_in_node(node, &mut ctx);
    ctx.output
}

fn locate_in_node(node: &OntosNode, ctx: &mut LocateCtx) {
    match &node.kind {
        NodeKind::VariableRef(var) => {
            if ctx.variables.contains(var.0 as usize)
                && ctx.output.insert(*var, ctx.current_path.clone()).is_some()
            {
                panic!("BUG: Variable appears more than once");
            }
        }
        NodeKind::Unit => {}
        NodeKind::Int(_) => {}
        NodeKind::Call(_, args) => locate_in_list(args, ctx),
        NodeKind::Seq(_, nodes) => locate_in_list(nodes, ctx),
        NodeKind::Struct(_, nodes) => locate_in_list(nodes, ctx),
        NodeKind::Prop(_, _, variant) => {
            ctx.enter_child(0, |ctx| locate_in_node(&variant.rel, ctx));
            ctx.enter_child(1, |ctx| locate_in_node(&variant.val, ctx));
        }
        NodeKind::MapSeq(..) => {
            todo!()
        }
        NodeKind::Destruct(_, nodes) => locate_in_list(nodes, ctx),
        NodeKind::MatchProp(_, _, _) => {
            unimplemented!("Cannot locate within MatchProp")
        }
    }
}

fn locate_in_list(nodes: &[OntosNode], ctx: &mut LocateCtx) {
    for (index, node) in nodes.iter().enumerate() {
        ctx.enter_child(index, |ctx| locate_in_node(node, ctx));
    }
}
