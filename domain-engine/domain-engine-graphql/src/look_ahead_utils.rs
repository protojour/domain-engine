use std::fmt::Display;

use juniper::{LookAheadArgument, LookAheadSelection, LookAheadValue};
use ontol_runtime::{
    attr::Attr,
    interface::{
        graphql::argument::{self, DefaultArg},
        serde::processor::ProcessorProfile,
    },
    sequence::Sequence,
    value::Value,
};
use serde::{
    de::{self, DeserializeSeed, IntoDeserializer},
    Deserialize,
};

use crate::{
    context::{SchemaCtx, ServiceCtx},
    field_error,
    gql_scalar::GqlScalar,
};

#[derive(Clone, Copy)]
pub enum EntityMutationKind {
    Create,
    Update,
    Delete,
}

pub struct EntityMutations {
    pub kind: EntityMutationKind,
    pub inputs: Sequence<Value>,
}

pub(crate) struct ArgsWrapper<'a> {
    selection: LookAheadSelection<'a, GqlScalar>,
}

impl<'a> ArgsWrapper<'a> {
    pub fn new(selection: LookAheadSelection<'a, GqlScalar>) -> Self {
        Self { selection }
    }

    pub fn deserialize_optional<'de, T: serde::Deserialize<'de>>(
        &self,
        name: &str,
    ) -> Result<Option<T>, juniper::FieldError<GqlScalar>> {
        match self.find_argument(name) {
            None => Ok(None),
            Some(look_ahead_arg) => {
                let value =
                    Option::<T>::deserialize(LookAheadValueDeserializer::from(look_ahead_arg))
                        .map_err(|error| field_error(format!("`{name}`: {error}")))?;

                Ok(value)
            }
        }
    }

    pub fn deserialize_domain_field_arg_as_attribute(
        &self,
        field_arg: &dyn argument::DomainFieldArg,
        (schema_ctx, service_ctx): (&SchemaCtx, &ServiceCtx),
    ) -> Result<Attr, juniper::FieldError<GqlScalar>> {
        let arg_name = field_arg.name(&schema_ctx.ontology);
        let (mode, level) = field_arg.typing_purpose().mode_and_level();

        let processor_profile =
            ProcessorProfile::default().with_flags(service_ctx.serde_processor_profile_flags);
        let serde_processor = schema_ctx
            .ontology
            .new_serde_processor(field_arg.operator_addr(), mode)
            .with_level(level)
            .with_profile(&processor_profile);

        let result = match self.find_argument(arg_name) {
            Some(look_ahead_arg) => {
                serde_processor.deserialize(LookAheadValueDeserializer::from(look_ahead_arg))
            }
            None => {
                let unlocated_span = juniper::Span::unlocated();
                let default_value: juniper::LookAheadValue<GqlScalar> = match field_arg
                    .default_arg()
                {
                    Some(DefaultArg::EmptyList) => LookAheadValue::List(Default::default()),
                    Some(DefaultArg::EmptyObject) => LookAheadValue::Object(Default::default()),
                    None => return Err(field_error(format!("argument `{arg_name}` is missing"))),
                };

                serde_processor.deserialize(LookAheadValueDeserializer {
                    value: default_value,
                    span: &unlocated_span,
                })
            }
        };

        result.map_err(field_error)
    }

    pub fn deserialize_entity_mutation_args(
        &self,
        create_arg: Option<&argument::EntityCreateInputsArg>,
        update_arg: Option<&argument::EntityUpdateInputsArg>,
        delete_arg: Option<&argument::EntityDeleteInputsArg>,
        (schema_ctx, service_ctx): (&SchemaCtx, &ServiceCtx),
    ) -> Result<Vec<EntityMutations>, juniper::FieldError<GqlScalar>> {
        // A uniform vector of the arguments that can be matched.
        // The point here is that we want to match them in the order that the user has specified
        // and return a vector that has the same order.
        let arg_matchers = {
            let mut arg_matchers: Vec<(&dyn argument::DomainFieldArg, EntityMutationKind)> = vec![];
            if let Some(create_arg) = create_arg {
                arg_matchers.push((create_arg, EntityMutationKind::Create));
            }
            if let Some(update_arg) = update_arg {
                arg_matchers.push((update_arg, EntityMutationKind::Update));
            }
            if let Some(delete_arg) = delete_arg {
                arg_matchers.push((delete_arg, EntityMutationKind::Delete));
            }
            arg_matchers
        };

        let processor_profile =
            ProcessorProfile::default().with_flags(service_ctx.serde_processor_profile_flags);

        let mut output = vec![];

        for look_ahead_arg in self.selection.arguments() {
            let name = look_ahead_arg.name();

            let Some((matched_arg, kind)) = arg_matchers
                .iter()
                .find(|(matching_arg, _)| matching_arg.name(&schema_ctx.ontology) == name)
                .cloned()
            else {
                // The GraphQL engine should have complained before reaching this point
                unreachable!();
            };

            let serde_processor = {
                let (mode, level) = matched_arg.typing_purpose().mode_and_level();
                schema_ctx
                    .ontology
                    .new_serde_processor(matched_arg.operator_addr(), mode)
                    .with_level(level)
                    .with_profile(&processor_profile)
            };

            let attr =
                serde_processor.deserialize(LookAheadValueDeserializer::from(look_ahead_arg))?;

            let inputs = match attr {
                Attr::Matrix(matrix) => matrix.columns.into_iter().next().unwrap(),
                _ => panic!("not a matrix"),
            };

            output.push(EntityMutations { kind, inputs });
        }

        Ok(output)
    }

    fn find_argument(&self, name: &str) -> Option<LookAheadArgument<'a, GqlScalar>> {
        self.selection.arguments().find(|arg| arg.name() == name)
    }
}

#[derive(Debug)]
struct Error {
    msg: String,
    start_pos: Option<juniper::parser::SourcePosition>,
}

impl Error {
    fn new(msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            start_pos: None,
        }
    }

    fn trailing_characters() -> Self {
        Self::new("trailing characters")
    }

    fn with_span(mut self, span: &juniper::Span) -> Self {
        self.set_span(span);
        self
    }

    fn set_span(&mut self, span: &juniper::Span) {
        if self.start_pos.is_none() && span.start != span.end {
            self.start_pos = Some(span.start);
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = &self.msg;
        if let Some(start_pos) = &self.start_pos {
            write!(
                f,
                "{msg} in input at line {} column {}",
                start_pos.line(),
                start_pos.column()
            )
        } else {
            write!(f, "{msg}")
        }
    }
}

impl de::StdError for Error {}

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self {
            msg: format!("{}", msg),
            start_pos: None,
        }
    }
}

trait ErrorContext {
    fn context(self, span: &juniper::Span) -> Self;
}

impl<T> ErrorContext for Result<T, Error> {
    fn context(self, span: &juniper::Span) -> Self {
        self.map_err(|mut error| {
            error.set_span(span);
            error
        })
    }
}

type BorrowedSpanning<'a, T> = juniper::Spanning<T, &'a juniper::Span>;

struct LookAheadValueDeserializer<'a> {
    value: LookAheadValue<'a, GqlScalar>,
    span: &'a juniper::Span,
}

impl<'a> From<juniper::LookAheadArgument<'a, GqlScalar>> for LookAheadValueDeserializer<'a> {
    fn from(value: juniper::LookAheadArgument<'a, GqlScalar>) -> Self {
        Self {
            value: value.value(),
            span: value.value_span(),
        }
    }
}

impl<'a, 'de> de::Deserializer<'de> for LookAheadValueDeserializer<'a> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            LookAheadValue::Null => visitor.visit_none::<Error>().context(self.span),
            LookAheadValue::Scalar(GqlScalar::I32(value)) => {
                visitor.visit_i32(*value).context(self.span)
            }
            LookAheadValue::Scalar(GqlScalar::I64(value)) => {
                visitor.visit_i64(*value).context(self.span)
            }
            LookAheadValue::Scalar(GqlScalar::F64(value)) => {
                visitor.visit_f64(*value).context(self.span)
            }
            LookAheadValue::Scalar(GqlScalar::Boolean(value)) => {
                visitor.visit_bool(*value).context(self.span)
            }
            LookAheadValue::Scalar(GqlScalar::String(value)) => {
                visitor.visit_str(value).context(self.span)
            }
            LookAheadValue::Enum(value) => visitor.visit_str(value),
            LookAheadValue::List(list) => {
                let mut iterator = list.iter().fuse();
                let value = visitor
                    .visit_seq(SeqDeserializer::<_>::new(&mut iterator))
                    .context(self.span)?;
                match iterator.next() {
                    Some(item) => Err(Error::trailing_characters().with_span(item.span)),
                    None => Ok(value),
                }
            }
            LookAheadValue::Object(object) => {
                let mut iterator = object.iter().fuse();
                let value = visitor
                    .visit_map(ObjectDeserializer::<_>::new(&mut iterator))
                    .context(self.span)?;
                match iterator.next() {
                    Some((key, _)) => Err(Error::trailing_characters().with_span(key.span)),
                    None => Ok(value),
                }
            }
        }
    }

    fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            LookAheadValue::Null => visitor.visit_none::<Error>().context(self.span),
            _ => {
                let span = self.span;
                visitor.visit_some(self).context(span)
            }
        }
    }

    serde::forward_to_deserialize_any!(
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        seq bytes byte_buf map unit_struct
        tuple_struct struct tuple ignored_any identifier newtype_struct enum
    );
}

struct SeqDeserializer<'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    count: usize,
}

impl<'i, I: Iterator> SeqDeserializer<'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self { iter, count: 0 }
    }
}

impl<'a, 'i, 'de, I> de::SeqAccess<'de> for SeqDeserializer<'i, I>
where
    I: Iterator<Item = BorrowedSpanning<'a, LookAheadValue<'a, GqlScalar>>>,
{
    type Error = Error;

    fn next_element_seed<V>(&mut self, seed: V) -> Result<Option<V::Value>, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(spanned_value) => {
                self.count += 1;
                seed.deserialize(LookAheadValueDeserializer {
                    value: spanned_value.item,
                    span: spanned_value.span,
                })
                .map(Some)
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.iter.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

struct ObjectDeserializer<'a, 'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    state: MapState<BorrowedSpanning<'a, LookAheadValue<'a, GqlScalar>>>,
    _count: usize,
}

impl<'a, 'i, I: Iterator> ObjectDeserializer<'a, 'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self {
            iter,
            state: MapState::NextKey,
            _count: 0,
        }
    }
}

impl<'a, 'i, 'de, I> de::MapAccess<'de> for ObjectDeserializer<'a, 'i, I>
where
    I: Iterator<
        Item = (
            BorrowedSpanning<'a, &'a str>,
            BorrowedSpanning<'a, LookAheadValue<'a, GqlScalar>>,
        ),
    >,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        match (self.iter.next(), &self.state) {
            (Some((spanned_key, spanned_value)), MapState::NextKey) => {
                self.state = MapState::NextValue(spanned_value);
                seed.deserialize(spanned_key.item.into_deserializer())
                    .map(Some)
            }
            (None, MapState::NextKey) => Ok(None),
            (_, MapState::NextValue(_)) => panic!("should call next_value"),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match std::mem::take(&mut self.state) {
            MapState::NextValue(spanned_value) => seed.deserialize(LookAheadValueDeserializer {
                value: spanned_value.item,
                span: spanned_value.span,
            }),
            MapState::NextKey => panic!("should call next_key"),
        }
    }
}

#[derive(Default)]
enum MapState<T> {
    #[default]
    NextKey,
    NextValue(T),
}
