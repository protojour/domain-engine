use ontol_parser::{
    cst::{
        inspect as insp,
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    lexer::{kind::Kind, unescape::unescape_regex},
    U32Span,
};
use ontol_runtime::{property::ValueCardinality, DefId};

use crate::{
    def::{TypeDef, TypeDefFlags},
    pattern::{
        CompoundPatternAttr, CompoundPatternAttrKind, CompoundPatternModifier, Pattern,
        PatternKind, SetBinaryOperator, SetPatternElement, SpreadLabel, TypePath,
    },
    regex_util::RegexToPatternLowerer,
    CompileError, SourceSpan,
};

use super::{
    context::{BlockContext, CstLowering, MapVarTable, SetElement},
    lower_misc::ReportError,
};

enum LoweredStructPatternParams {
    Attrs {
        attrs: Box<[CompoundPatternAttr]>,
        spread_label: Option<Box<SpreadLabel>>,
    },
    Unit(Pattern),
    Error,
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn lower_pattern(
        &mut self,
        pat: insp::Pattern<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        match pat {
            insp::Pattern::PatStruct(pat) => self.lower_struct_pattern(pat, var_table),
            insp::Pattern::PatSet(pat) => self.lower_set_pattern(pat, var_table),
            insp::Pattern::PatAtom(pat) => {
                let span = pat.0.span();
                self.lower_atom_pattern(pat, var_table)
                    .unwrap_or_else(|| self.mk_error_pattern(span))
            }
            insp::Pattern::PatBinary(pat) => self.lower_binary_pattern(pat, var_table),
        }
    }

    fn lower_struct_pattern(
        &mut self,
        pat_struct: insp::PatStruct<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        let span = pat_struct.0.span();
        let type_path = match pat_struct.ident_path() {
            Some(ident_path) => match self.lookup_path(&ident_path, ReportError::Yes) {
                Some(def_id) => TypePath::Specified {
                    def_id,
                    span: self.ctx.source_span(ident_path.0.span()),
                },
                None => return self.ctx.mk_pattern(PatternKind::Error, span),
            },
            None => {
                let def_id = self.ctx.define_anonymous_type(
                    TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
                    },
                    span,
                );
                self.ctx
                    .compiler
                    .namespaces
                    .add_anonymous(self.ctx.package_id, def_id);

                TypePath::Inferred { def_id }
            }
        };

        match self.lower_struct_pattern_params(pat_struct.params(), var_table) {
            LoweredStructPatternParams::Attrs {
                attrs,
                spread_label,
            } => {
                let mut modifier = None;
                for m in pat_struct.modifiers() {
                    if m.slice() == "@match" {
                        modifier = Some(CompoundPatternModifier::Match);
                    } else {
                        CompileError::InvalidModifier.span_report(m.span(), &mut self.ctx);
                    }
                }

                self.ctx.mk_pattern(
                    PatternKind::Compound {
                        type_path,
                        modifier,
                        is_unit_binding: false,
                        attributes: attrs,
                        spread_label,
                    },
                    span,
                )
            }
            LoweredStructPatternParams::Unit(unit_pattern) => {
                if let Some(modifier) = pat_struct.modifiers().next() {
                    CompileError::TODO("modifier not supported here")
                        .span_report(modifier.span(), &mut self.ctx);
                }

                let key = (DefId::unit(), self.ctx.source_span(span));

                self.ctx.mk_pattern(
                    PatternKind::Compound {
                        type_path,
                        modifier: None,
                        is_unit_binding: true,
                        attributes: [CompoundPatternAttr {
                            key,
                            bind_option: None,
                            kind: CompoundPatternAttrKind::Value {
                                rel: None,
                                val: unit_pattern,
                            },
                        }]
                        .into(),
                        spread_label: None,
                    },
                    span,
                )
            }
            LoweredStructPatternParams::Error => self.ctx.mk_pattern(PatternKind::Error, span),
        }
    }

    fn lower_struct_pattern_params(
        &mut self,
        params: impl Iterator<Item = insp::StructParam<V>>,
        var_table: &mut MapVarTable,
    ) -> LoweredStructPatternParams {
        let mut attrs: Vec<CompoundPatternAttr> = vec![];
        let mut spread_label: Option<Box<SpreadLabel>> = None;
        let mut spread_error_reported = false;

        for param in params {
            if spread_label.is_some() && !spread_error_reported {
                CompileError::SpreadLabelMustBeLastArgument
                    .span_report(param.view().span(), &mut self.ctx);
                spread_error_reported = true;
            }

            match param {
                insp::StructParam::StructParamAttrProp(attr_prop) => {
                    if let Some(attr) = self.lower_compound_pattern_attr_prop(attr_prop, var_table)
                    {
                        attrs.push(attr);
                    }
                }
                insp::StructParam::Spread(spread) => {
                    let Some(symbol) = spread.symbol() else {
                        return LoweredStructPatternParams::Error;
                    };
                    spread_label = Some(Box::new(SpreadLabel(
                        symbol.slice().to_string(),
                        self.ctx.source_span(symbol.span()),
                    )));
                }
                insp::StructParam::StructParamAttrUnit(attr_unit) => {
                    let unit_pattern = match attr_unit.pattern() {
                        Some(insp::Pattern::PatSet(pat_set)) => {
                            CompileError::TODO("set pattern not allowed here")
                                .span_report(pat_set.0.span(), &mut self.ctx);
                            return LoweredStructPatternParams::Error;
                        }
                        None => return LoweredStructPatternParams::Error,
                        Some(pattern) => self.lower_pattern(pattern, var_table),
                    };
                    return LoweredStructPatternParams::Unit(unit_pattern);
                }
            }
        }

        LoweredStructPatternParams::Attrs {
            attrs: attrs.into_boxed_slice(),
            spread_label,
        }
    }

    fn lower_compound_pattern_attr_prop(
        &mut self,
        attr_prop: insp::StructParamAttrProp<V>,
        var_table: &mut MapVarTable,
    ) -> Option<CompoundPatternAttr> {
        let relation = attr_prop.relation()?;
        let relation_span = self.ctx.source_span(relation.view().span());
        let relation_def = self
            .resolve_type_reference(
                relation.type_ref()?,
                ValueCardinality::Unit,
                &BlockContext::NoContext,
                ReportError::Yes,
                None,
            )?
            .def_id;
        let bind_option = attr_prop
            .prop_cardinality()
            .and_then(|pc| pc.question())
            .map(|q| self.ctx.source_span(q.span()));

        let Some(cst_pattern) = attr_prop.pattern() else {
            return Some(CompoundPatternAttr {
                key: (relation_def, relation_span),
                bind_option,
                kind: CompoundPatternAttrKind::Value {
                    rel: None,
                    val: self.mk_error_pattern(attr_prop.0.span()),
                },
            });
        };

        match cst_pattern {
            insp::Pattern::PatSet(pat_set) => {
                if let Some(rel_args) = attr_prop.rel_args() {
                    CompileError::TODO("relation arguments must be associated with each element")
                        .span_report(rel_args.0.span(), &mut self.ctx);
                    return None;
                }

                let kind = match self.get_set_binary_operator(pat_set.modifier()) {
                    Some((operator, _span)) => {
                        self.lower_set_algebra_pattern(pat_set, operator, var_table)?
                    }
                    None => {
                        let object_pattern = self.lower_pattern(pat_set.into(), var_table);

                        CompoundPatternAttrKind::Value {
                            rel: None,
                            val: object_pattern,
                        }
                    }
                };

                Some(CompoundPatternAttr {
                    key: (relation_def, relation_span),
                    bind_option,
                    kind,
                })
            }
            obj_pattern => {
                let object_pattern = self.lower_pattern(obj_pattern, var_table);
                let rel = self.lower_compound_pattern_attr_rel_args(
                    attr_prop.rel_args(),
                    &object_pattern,
                    var_table,
                )?;

                Some(CompoundPatternAttr {
                    key: (relation_def, relation_span),
                    bind_option,
                    kind: CompoundPatternAttrKind::Value {
                        rel: rel.map(Box::new),
                        val: object_pattern,
                    },
                })
            }
        }
    }

    fn lower_compound_pattern_attr_rel_args(
        &mut self,
        rel_args: Option<insp::RelArgs<V>>,
        object_pattern: &Pattern,
        var_table: &mut MapVarTable,
    ) -> Option<Option<Pattern>> {
        let Some(rel_args) = rel_args else {
            return Some(None);
        };
        // Inherit modifier from object pattern
        let modifier = match &object_pattern {
            Pattern {
                kind: PatternKind::Compound { modifier, .. },
                ..
            } => *modifier,
            _ => None,
        };

        match self.lower_struct_pattern_params(rel_args.params(), var_table) {
            LoweredStructPatternParams::Attrs {
                attrs,
                spread_label,
            } => Some(Some(self.ctx.mk_pattern(
                PatternKind::Compound {
                    type_path: TypePath::RelContextual,
                    modifier,
                    is_unit_binding: false,
                    attributes: attrs,
                    spread_label,
                },
                rel_args.0.span(),
            ))),
            LoweredStructPatternParams::Unit(unit_pattern) => {
                CompileError::TODO("unit not supported here")
                    .span(unit_pattern.span)
                    .report(self.ctx.compiler);
                None
            }
            LoweredStructPatternParams::Error => {
                Some(Some(self.mk_error_pattern(rel_args.0.span())))
            }
        }
    }

    fn lower_set_pattern(
        &mut self,
        pat_set: insp::PatSet<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        let seq_type = pat_set
            .ident_path()
            .and_then(|ident_path| self.lookup_path(&ident_path, ReportError::Yes));

        let mut pattern_elements = vec![];
        for element in pat_set.elements() {
            let pattern = element
                .pattern()
                .map(|pattern| self.lower_pattern(pattern, var_table))
                .unwrap_or_else(|| self.mk_error_pattern(element.0.span()));

            let rel = if let Some(rel_args) = element.rel_args() {
                let mut lowered_attrs = vec![];

                let mut spread_label = None;

                for param in rel_args.params() {
                    match param {
                        insp::StructParam::StructParamAttrProp(attr_prop) => {
                            if let Some(attr_prop) =
                                self.lower_compound_pattern_attr_prop(attr_prop, var_table)
                            {
                                lowered_attrs.push(attr_prop);
                            }
                        }
                        insp::StructParam::StructParamAttrUnit(unit) => {
                            CompileError::TODO("unit cannot be used here")
                                .span_report(unit.0.span(), &mut self.ctx);
                        }
                        insp::StructParam::Spread(spread) => {
                            if let Some(symbol) = spread.symbol() {
                                spread_label = Some(Box::new(SpreadLabel(
                                    symbol.slice().to_string(),
                                    self.ctx.source_span(symbol.span()),
                                )));
                            }
                        }
                    }
                }

                Some(Pattern {
                    id: self.ctx.compiler.patterns.alloc_pat_id(),
                    kind: PatternKind::Compound {
                        type_path: TypePath::RelContextual,
                        modifier: None,
                        is_unit_binding: false,
                        attributes: lowered_attrs.into(),
                        spread_label,
                    },
                    span: self.ctx.source_span(rel_args.0.span()),
                })
            } else {
                None
            };

            pattern_elements.push(SetPatternElement {
                id: self.ctx.compiler.patterns.alloc_pat_id(),
                is_iter: element.spread().is_some(),
                rel,
                val: pattern,
            })
        }

        self.ctx.mk_pattern(
            PatternKind::Set {
                val_type_def: seq_type,
                elements: pattern_elements.into_boxed_slice(),
            },
            pat_set.0.span(),
        )
    }

    fn lower_set_algebra_pattern(
        &mut self,
        pat_set: insp::PatSet<V>,
        operator: SetBinaryOperator,
        var_table: &mut MapVarTable,
    ) -> Option<CompoundPatternAttrKind> {
        let mut elements = vec![];

        for element in pat_set.elements() {
            let rel = match element.rel_args() {
                Some(rel_args) => {
                    let params = self.lower_struct_pattern_params(rel_args.params(), var_table);
                    Some(self.ctx.mk_pattern(
                        PatternKind::Compound {
                            type_path: TypePath::RelContextual,
                            modifier: None,
                            is_unit_binding: false,
                            attributes: match params {
                                LoweredStructPatternParams::Attrs { attrs, .. } => attrs,
                                _ => {
                                    continue;
                                }
                            },
                            spread_label: None,
                        },
                        rel_args.0.span(),
                    ))
                }
                None => None,
            };
            let val = element
                .pattern()
                .map(|pattern| self.lower_pattern(pattern, var_table))
                .unwrap_or_else(|| self.mk_error_pattern(element.0.span()));

            elements.push(SetElement {
                iter: element.spread().is_some(),
                rel,
                val,
            });
        }

        if elements.is_empty() {
            CompileError::TODO("inner set must have at least one element")
                .span_report(pat_set.0.span(), &mut self.ctx);
        }

        Some(CompoundPatternAttrKind::SetOperator {
            operator,
            elements: elements
                .into_iter()
                .map(|SetElement { iter, rel, val }| SetPatternElement {
                    id: self.ctx.compiler.patterns.alloc_pat_id(),
                    is_iter: iter,
                    rel,
                    val,
                })
                .collect(),
        })
    }

    fn lower_atom_pattern(
        &mut self,
        pat_atom: insp::PatAtom<V>,
        var_table: &mut MapVarTable,
    ) -> Option<Pattern> {
        let token = pat_atom.0.local_tokens().next()?;
        let span = token.span();

        match token.kind() {
            Kind::Number => match token.slice().parse::<i64>() {
                Ok(int) => Some(self.ctx.mk_pattern(PatternKind::ConstInt(int), span)),
                Err(_) => {
                    CompileError::InvalidInteger.span_report(span, &mut self.ctx);
                    None
                }
            },
            Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                let text = self.ctx.unescape(token.literal_text()?)?;
                Some(self.ctx.mk_pattern(PatternKind::ConstText(text), span))
            }
            Kind::Regex => {
                let regex_literal = unescape_regex(token.slice());
                let regex_def_id = match self.ctx.compiler.defs.def_regex(
                    &regex_literal,
                    span,
                    &mut self.ctx.compiler.str_ctx,
                ) {
                    Ok(def_id) => def_id,
                    Err((compile_error, err_span)) => {
                        CompileError::InvalidRegex(compile_error)
                            .span_report(err_span, &mut self.ctx);
                        return None;
                    }
                };
                let regex_meta = self
                    .ctx
                    .compiler
                    .defs
                    .literal_regex_meta_table
                    .get(&regex_def_id)
                    .unwrap();

                let mut regex_lowerer = RegexToPatternLowerer::new(
                    regex_meta.pattern,
                    span,
                    self.ctx.source_id,
                    var_table,
                    &mut self.ctx.compiler.patterns,
                );

                regex_syntax::ast::visit(&regex_meta.ast, regex_lowerer.syntax_visitor()).unwrap();
                regex_syntax::hir::visit(&regex_meta.hir, regex_lowerer.syntax_visitor()).unwrap();

                let expr_regex = regex_lowerer.into_expr(regex_def_id);

                Some(self.ctx.mk_pattern(PatternKind::Regex(expr_regex), span))
            }
            Kind::Symbol => match token.slice() {
                "true" => Some(self.ctx.mk_pattern(PatternKind::ConstBool(true), span)),
                "false" => Some(self.ctx.mk_pattern(PatternKind::ConstBool(false), span)),
                ident => {
                    let var = var_table.get_or_create_var(ident.to_string());
                    Some(self.ctx.mk_pattern(PatternKind::Variable(var), span))
                }
            },
            _ => None,
        }
    }

    fn lower_binary_pattern(
        &mut self,
        pat_atom: insp::PatBinary<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        let span = pat_atom.0.span();
        let mut operands = pat_atom.operands();
        let left = operands
            .next()
            .map(|op| self.lower_pattern(op, var_table))
            .unwrap_or_else(|| self.mk_error_pattern(span));

        let Some(infix_token) = pat_atom.infix_token() else {
            return self.mk_error_pattern(span);
        };

        let fn_ident = match infix_token.kind() {
            Kind::Plus => "+",
            Kind::Minus => "-",
            Kind::Star => "*",
            Kind::Div => "/",
            _ => {
                CompileError::TODO("invalid infix operator")
                    .span_report(infix_token.span(), &mut self.ctx);
                return self.mk_error_pattern(span);
            }
        };

        let Some(def_id) = self.catch(|zelf| zelf.ctx.lookup_ident(fn_ident, infix_token.span()))
        else {
            return self.mk_error_pattern(span);
        };

        let right = operands
            .next()
            .map(|op| self.lower_pattern(op, var_table))
            .unwrap_or_else(|| self.mk_error_pattern(span));

        self.ctx.mk_pattern(
            PatternKind::Call(def_id, Box::new([left, right])),
            pat_atom.0.span(),
        )
    }

    fn get_set_binary_operator(
        &mut self,
        modifier: Option<V::Token>,
    ) -> Option<(SetBinaryOperator, SourceSpan)> {
        let modifier = modifier?;
        let span = self.ctx.source_span(modifier.span());

        match modifier.slice() {
            "@in" => Some((SetBinaryOperator::ElementIn, span)),
            "@all_in" => Some((SetBinaryOperator::AllIn, span)),
            "@contains_all" => Some((SetBinaryOperator::ContainsAll, span)),
            "@intersects" => Some((SetBinaryOperator::Intersects, span)),
            "@equals" => Some((SetBinaryOperator::SetEquals, span)),
            _ => {
                CompileError::TODO("invalid set binary operator")
                    .span_report(modifier.span(), &mut self.ctx);
                None
            }
        }
    }

    pub(super) fn mk_error_pattern(&mut self, span: U32Span) -> Pattern {
        self.ctx.mk_pattern(PatternKind::Error, span)
    }
}
