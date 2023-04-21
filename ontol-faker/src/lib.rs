use std::collections::BTreeMap;

use fake::{Fake, Faker};
use ontol_runtime::{
    env::Env,
    serde::{
        operator::{FilteredVariants, SerdeOperator},
        processor::{ProcessorLevel, ProcessorMode, SerdeProcessor},
    },
    value::{Attribute, Data, Value},
    DefId,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tracing::debug;

const MAX_REPEAT: u32 = 128;

#[derive(Debug)]
pub enum Error {
    NoSerializationInfo,
}

pub fn new_constant_fake(
    env: &Env,
    def_id: DefId,
    processor_mode: ProcessorMode,
) -> Result<Value, Error> {
    let seed = [
        1, 0, 0, 0, 23, 0, 0, 0, 200, 1, 0, 0, 210, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];
    let mut rng = StdRng::from_seed(seed);
    FakeGenerator {
        env,
        rng: &mut rng,
        processor_mode,
    }
    .fake_value(def_id)
}

struct FakeGenerator<'a, R: Rng> {
    env: &'a Env,
    rng: &'a mut R,
    processor_mode: ProcessorMode,
}

impl<'a, R: Rng> FakeGenerator<'a, R> {
    pub fn fake_value(&mut self, def_id: DefId) -> Result<Value, Error> {
        let type_info = self.env.get_type_info(def_id);
        let operator_id = type_info.operator_id.ok_or(Error::NoSerializationInfo)?;

        Ok(self
            .fake_attribute(self.env.new_serde_processor(
                operator_id,
                None,
                self.processor_mode,
                ProcessorLevel::Root,
            ))
            .value)
    }

    fn fake_attribute(&mut self, processor: SerdeProcessor) -> Attribute {
        let value = match processor.value_operator {
            SerdeOperator::Unit => Value::unit(),
            SerdeOperator::False(def_id) => Value::new(Data::Int(0), *def_id),
            SerdeOperator::True(def_id) => Value::new(Data::Int(1), *def_id),
            SerdeOperator::Bool(def_id) => {
                let value: bool = Faker.fake_with_rng(self.rng);
                Value::new(Data::Int(if value { 1 } else { 0 }), *def_id)
            }
            SerdeOperator::Int(def_id) => {
                let int: i32 = Faker.fake_with_rng(self.rng);
                Value::new(Data::Int(int.into()), *def_id)
            }
            SerdeOperator::Number(def_id) => Value::new(
                Data::new_rational_i64(
                    Faker.fake_with_rng(self.rng),
                    match Faker.fake_with_rng(self.rng) {
                        0 => 1,
                        other => other,
                    },
                ),
                *def_id,
            ),
            SerdeOperator::String(def_id) => {
                let string: std::string::String =
                    fake::faker::name::en::Name().fake_with_rng(self.rng);
                Value::new(Data::String(string.into()), *def_id)
            }
            SerdeOperator::StringConstant(s, def_id) => {
                Value::new(Data::String(s.clone()), *def_id)
            }
            SerdeOperator::StringPattern(def_id) => {
                let pattern = self.env.get_string_pattern(*def_id).unwrap();
                let rand_regex =
                    rand_regex::Regex::compile(pattern.regex.as_str(), MAX_REPEAT).unwrap();
                let string: std::string::String = self.rng.sample(&rand_regex);
                Value::new(Data::String(string.into()), *def_id)
            }
            SerdeOperator::CapturingStringPattern(def_id) => {
                let string_pattern = self.env.get_string_pattern(*def_id).unwrap();
                let mut parser = regex_syntax::ast::parse::Parser::new();

                if let Some(ast) =
                    transform_regex(&parser.parse(string_pattern.regex.as_str()).unwrap())
                {
                    let ast_string = ast.to_string();
                    let hir = regex_syntax::hir::translate::Translator::new()
                        .translate(&ast_string, &ast)
                        .unwrap();
                    let regex_string = hir.to_string();

                    debug!("fake for regex: {regex_string}");

                    let rand_regex = rand_regex::Regex::compile(&regex_string, MAX_REPEAT).unwrap();

                    let string: std::string::String = self.rng.sample(&rand_regex);

                    let data = string_pattern
                        .try_capturing_match(&string, self.env)
                        .unwrap();

                    Value::new(data, *def_id)
                } else {
                    panic!()
                }
            }
            SerdeOperator::RelationSequence(seq_op) => {
                let variant = &seq_op.ranges[0];
                let attr = self.fake_attribute(processor.narrow(variant.operator_id));

                return Value::new(Data::Sequence([attr].into()), seq_op.def_variant.def_id).into();
            }
            SerdeOperator::ConstructorSequence(seq_op) => {
                let mut seq = vec![];

                for range in &seq_op.ranges {
                    if let Some(rep) = range.finite_repetition {
                        for _ in 0..(rep as usize) {
                            seq.push(self.fake_attribute(processor.new_child(range.operator_id)));
                        }
                    } else {
                        seq.push(self.fake_attribute(processor.new_child(range.operator_id)));
                    }
                }

                Value::new(Data::Sequence(seq), seq_op.def_variant.def_id)
            }
            SerdeOperator::ValueType(value_op) => {
                return self.fake_attribute(processor.narrow(value_op.inner_operator_id));
            }
            SerdeOperator::Union(union_op) => {
                return match union_op.variants(self.processor_mode, processor.level()) {
                    FilteredVariants::Single(id) => self.fake_attribute(processor.narrow(id)),
                    FilteredVariants::Multi(variants) => {
                        let index: usize = self.rng.gen_range(0..variants.len());
                        let variant = &variants[index];

                        self.fake_attribute(processor.narrow(variant.operator_id))
                    }
                }
            }
            SerdeOperator::PrimaryId(_name, inner_operator_id) => {
                return self.fake_attribute(processor.narrow(*inner_operator_id))
            }
            SerdeOperator::Struct(struct_op) => {
                let mut attrs = BTreeMap::default();
                for (_, property) in &struct_op.properties {
                    let attr = self.fake_attribute(processor.new_child(property.value_operator_id));
                    attrs.insert(property.property_id, attr);
                }

                Value::new(Data::Struct(attrs), struct_op.def_variant.def_id)
            }
        };

        let rel_params = if let Some(rel_params_operator_id) = processor.rel_params_operator_id {
            self.fake_attribute(processor.new_child(rel_params_operator_id))
                .value
        } else {
            Value::unit()
        };

        Attribute { value, rel_params }
    }
}

fn transform_regex(ast: &regex_syntax::ast::Ast) -> Option<regex_syntax::ast::Ast> {
    use regex_syntax::ast::*;
    match ast {
        Ast::Group(Group { span, kind: _, ast }) => Some(Ast::Group(Group {
            span: *span,
            kind: GroupKind::NonCapturing(Flags {
                span: Span::new(Position::new(0, 0, 0), Position::new(0, 0, 0)),
                items: vec![],
            }),
            ast: ast.clone(),
        })),
        // Anchors are not supported by rand_regex
        Ast::Assertion(_) => None,
        Ast::Repetition(Repetition {
            span,
            op,
            greedy,
            ast,
        }) => Some(Ast::Repetition(Repetition {
            span: *span,
            op: op.clone(),
            greedy: *greedy,
            ast: Box::new(transform_regex(ast)?),
        })),
        Ast::Alternation(Alternation { span, asts }) => Some(Ast::Alternation(Alternation {
            span: *span,
            asts: asts.iter().filter_map(transform_regex).collect(),
        })),
        Ast::Concat(Concat { span, asts }) => Some(Ast::Concat(Concat {
            span: *span,
            asts: asts.iter().filter_map(transform_regex).collect(),
        })),
        other => Some(other.clone()),
    }
}
