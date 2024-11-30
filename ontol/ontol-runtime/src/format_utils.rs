use std::fmt::{Debug, Display};

use crate::{
    attr::AttrRef,
    interface::serde::processor::{ProcessorMode, SerdeProcessor},
    ontology::aspects::{DefsAspect, ExecutionAspect, SerdeAspect},
    value::{Value, ValueFormatRaw},
};

pub struct LogicalConcat<T> {
    pub items: Vec<T>,
    pub logic_op: LogicOp,
}

impl<T: Display> Display for LogicalConcat<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.items.as_slice() {
            [] => panic!("BUG: Nothing is missing!"),
            [single] => write!(f, "{single}"),
            [a, b] => write!(f, "{a} {} {b}", self.logic_op.name()),
            slice => {
                write!(
                    f,
                    "{} {}",
                    match self.logic_op {
                        LogicOp::And => "all of",
                        LogicOp::Or => "one of",
                    },
                    CommaSeparated(slice)
                )
            }
        }
    }
}

pub enum LogicOp {
    And,
    Or,
}

impl LogicOp {
    fn name(&self) -> &'static str {
        match self {
            Self::And => "and",
            Self::Or => "or",
        }
    }
}

pub struct DoubleQuote<T>(pub T);

impl<T: Display> Display for DoubleQuote<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\"", self.0)
    }
}

pub struct Backticks<T>(pub T);

impl<T: Display> Display for Backticks<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{}`", self.0)
    }
}

pub struct CommaSeparated<I>(pub I);

impl<I, T> Display for CommaSeparated<I>
where
    I: IntoIterator<Item = T> + Copy,
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iterator = self.0.into_iter().peekable();
        while let Some(next) = iterator.next() {
            write!(f, "{next}")?;
            if iterator.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

/// Maps a display operator for use with e.g. CommaSeparated.
/// I is some IntoIterator, F is a function.
#[derive(Clone, Copy)]
pub struct FmtMap<I, F>(pub I, pub F);

impl<I, F, Item, D> IntoIterator for FmtMap<I, F>
where
    I: IntoIterator<Item = Item>,
    F: FnMut(Item) -> D,
{
    type Item = D;
    type IntoIter = std::iter::Map<<I as IntoIterator>::IntoIter, F>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter().map(self.1)
    }
}

#[derive(Default, Clone, Copy)]
pub struct Indent(usize);

impl Indent {
    pub fn inc(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for Indent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for _ in 0..self.0 {
            write!(f, "  ")?;
        }
        Ok(())
    }
}

pub struct DebugViaDisplay<'a, T>(pub &'a T);

impl<T> Debug for DebugViaDisplay<'_, T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn try_alpha_to_u32(sym: &str, base: char) -> Option<u32> {
    if sym.is_empty() {
        return None;
    }

    let mut num: u32 = 0;
    let mut iterator = sym.chars().peekable();

    while let Some(char) = iterator.next() {
        if !char.is_ascii_lowercase() {
            return None;
        }

        let value = u32::from(char) - u32::from(base);
        num += value;

        if iterator.peek().is_some() {
            num = (num + 1) * 26;
        }
    }

    Some(num)
}

pub struct AsAlpha(pub u32, pub char);

impl Display for AsAlpha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 >= 26 {
            write!(f, "{}", AsAlpha((self.0 / 26) - 1, self.1))?;
        }

        let rem = self.0 % 26;
        write!(f, "{}", char::from_u32(u32::from(self.1) + rem).unwrap())
    }
}

pub struct Literal<'a>(pub &'a str);

impl Display for Literal<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Literal<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub enum FormattedValue<'v> {
    Str(&'v str),
    String(String),
    JsonFallback(String),
    JsonError(serde_json::Error),
}

impl FormattedValue<'_> {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FormattedValue::Str(s) => Some(s),
            FormattedValue::String(s) => Some(s.as_str()),
            FormattedValue::JsonFallback(s) => Some(s.as_str()),
            FormattedValue::JsonError(_error) => None,
        }
    }
}

impl Display for FormattedValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormattedValue::Str(s) => write!(f, "{s}"),
            FormattedValue::String(s) => write!(f, "{s}"),
            FormattedValue::JsonFallback(s) => write!(f, "{s}"),
            FormattedValue::JsonError(_error) => write!(f, "N/A"),
        }
    }
}

/// best-effort string-formatting of a value, in accordance with domain definition.
///
/// If the value has a string-based serialization, that string is returned.
/// If the value has a complex serialization that's not string, a JSON-as-string representation is returned.
pub fn format_value<'v>(
    value: &'v Value,
    ontology: &(impl AsRef<DefsAspect> + AsRef<SerdeAspect>),
) -> FormattedValue<'v> {
    if let Value::Text(text, _) = value {
        // ASSUMPTION: text values always have a transparent representation:
        FormattedValue::Str(text)
    } else {
        let defs: &DefsAspect = ontology.as_ref();
        let def = defs.def(value.type_def_id());

        if let Some(operator_addr) = def.operator_addr {
            let dummy_execution = ExecutionAspect::empty();
            let ctx = crate::interface::serde::OntologyCtx {
                serde: ontology.as_ref(),
                defs: ontology.as_ref(),
                execution: &dummy_execution,
            };
            let processor = SerdeProcessor::new(operator_addr, ProcessorMode::Read, &ctx);

            match processor.serialize_attr(AttrRef::Unit(value), serde_json::value::Serializer) {
                Ok(serde_json::Value::String(string)) => FormattedValue::String(string),
                Ok(json_value) => serde_json::to_string(&json_value)
                    .map(FormattedValue::JsonFallback)
                    .unwrap_or_else(FormattedValue::JsonError),
                Err(err) => {
                    tracing::error!(
                        "failed to format_value {value:?} via serde: {err:?}, formatting as `N/A`"
                    );
                    FormattedValue::JsonError(err)
                }
            }
        } else {
            FormattedValue::String(format!(
                "{}",
                ValueFormatRaw::new(value, value.type_def_id(), ontology.as_ref()),
            ))
        }
    }
}

#[test]
fn test_as_alpha() {
    assert_eq!("a", format!("{}", AsAlpha(0, 'a')));
    assert_eq!("z", format!("{}", AsAlpha(25, 'a')));
    assert_eq!("aa", format!("{}", AsAlpha(26, 'a')));
    assert_eq!("az", format!("{}", AsAlpha(51, 'a')));
    assert_eq!("ba", format!("{}", AsAlpha(52, 'a')));
    assert_eq!("YQ", format!("{}", AsAlpha(666, 'A')));
}
