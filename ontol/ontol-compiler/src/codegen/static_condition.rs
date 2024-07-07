use ontol_hir::{Kind, Node, PropVariant};
use ontol_runtime::{
    interface::discriminator::{Discriminant, LeafDiscriminant},
    query::condition::{Clause, CondTerm, Condition, SetOperator},
    value::Value,
    var::Var,
};
use tracing::debug;

use crate::{
    typed_hir::{TypedArena, TypedRootNode},
    CompileError, CompileErrors, Compiler,
};

pub fn generate_static_condition_from_scope<'m>(
    scope_node: &TypedRootNode<'m>,
    compiler: &mut Compiler<'m>,
) -> Condition {
    let mut builder = ConditionBuilder {
        arena: scope_node.arena(),
        output: Condition::default(),
        compiler,
        errors: CompileErrors::default(),
    };

    let term = builder.term(scope_node.node(), None);

    if let CondTerm::Variable(root_var) = term {
        builder.output.add_clause(root_var, Clause::Root);
    }

    let output = builder.output;
    compiler.errors.extend(builder.errors);
    output
}

struct ConditionBuilder<'c, 'm> {
    arena: &'c TypedArena<'m>,
    output: Condition,
    compiler: &'c Compiler<'m>,
    errors: CompileErrors,
}

impl<'c, 'm> AsMut<CompileErrors> for ConditionBuilder<'c, 'm> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        &mut self.errors
    }
}

impl<'c, 'm> ConditionBuilder<'c, 'm> {
    fn term(&mut self, node: Node, parent_var: Option<Var>) -> CondTerm {
        match self.arena[node].hir() {
            Kind::Struct(_, _, nodes) => {
                let var = self.output.mk_cond_var();
                for node in nodes {
                    self.term(*node, Some(var));
                }
                CondTerm::Variable(var)
            }
            Kind::Prop(_, _, prop_id, variant) => match variant {
                PropVariant::Predicate(..) => CondTerm::Wildcard,
                PropVariant::Unit(node) => {
                    let Some(parent_var) = parent_var else {
                        return CondTerm::Wildcard;
                    };
                    let prop_var = self.output.mk_cond_var();

                    let term = self.term(*node, Some(prop_var));

                    if !matches!(term, CondTerm::Wildcard) {
                        self.output.add_clause(
                            parent_var,
                            Clause::MatchProp(*prop_id, SetOperator::ElementIn, prop_var),
                        );

                        self.output
                            .add_clause(prop_var, Clause::Member(CondTerm::Wildcard, term));
                    }

                    CondTerm::Wildcard
                }
                PropVariant::Tuple(tup) => {
                    let Some(parent_var) = parent_var else {
                        return CondTerm::Wildcard;
                    };
                    let prop_var = self.output.mk_cond_var();

                    if tup.len() != 2 {
                        panic!("only 2-tuples supported for now");
                    }

                    let val = self.term(tup[0], Some(prop_var));
                    let rel = self.term(tup[1], Some(prop_var));

                    if !matches!(rel, CondTerm::Wildcard) || !matches!(val, CondTerm::Wildcard) {
                        self.output.add_clause(
                            parent_var,
                            Clause::MatchProp(*prop_id, SetOperator::ElementIn, prop_var),
                        );

                        self.output.add_clause(prop_var, Clause::Member(rel, val));
                    }

                    CondTerm::Wildcard
                }
            },
            Kind::Narrow(inner) => {
                let Some(pre_narrowed_def_id) = self.arena[node].ty().get_single_def_id() else {
                    return CondTerm::Wildcard;
                };
                let Some(narrowed_def_id) = self.arena[*inner].ty().get_single_def_id() else {
                    return CondTerm::Wildcard;
                };

                let narrow_var = self.output.mk_cond_var();

                debug!("narrow {pre_narrowed_def_id:?} to {narrowed_def_id:?}");

                let discr = self
                    .compiler
                    .rel_ctx
                    .union_discriminators
                    .get(&pre_narrowed_def_id)
                    .expect("narrowing must be union-based");
                let variant = discr
                    .variants
                    .iter()
                    .find(|variant| variant.def_id == narrowed_def_id)
                    .expect("union variant not found");

                match &variant.discriminator.discriminant {
                    Discriminant::HasAttribute(relationship_id, _, _, leaf) => {
                        let variant_var = self.output.mk_cond_var();

                        self.output.add_clause(
                            narrow_var,
                            Clause::MatchProp(
                                *relationship_id,
                                SetOperator::ElementIn,
                                variant_var,
                            ),
                        );

                        if let LeafDiscriminant::IsTextLiteral(constant) = leaf {
                            self.output.add_clause(
                                variant_var,
                                Clause::Member(
                                    CondTerm::Wildcard,
                                    CondTerm::Value(Value::Text(
                                        self.compiler.str_ctx[*constant].into(),
                                        self.compiler.primitives.text.into(),
                                    )),
                                ),
                            );
                        } else {
                            CompileError::BUG(
                                "static condition: unhandled leaf discriminant for has-attribute",
                            )
                            .span(self.arena[node].span())
                            .report(self)
                        }
                    }
                    _ => {
                        CompileError::BUG("static condition: unhandled discriminant")
                            .span(self.arena[node].span())
                            .report(self);
                    }
                }

                self.term(*inner, Some(narrow_var));

                CondTerm::Variable(narrow_var)
            }
            // TODO: Maybe support more variations
            _ => CondTerm::Wildcard,
        }
    }
}
