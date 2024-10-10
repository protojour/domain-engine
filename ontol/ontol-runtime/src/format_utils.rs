use std::fmt::{Debug, Display};

use crate::{
    attr::AttrRef,
    interface::serde::processor::{ProcessorMode, SerdeProcessor},
    ontology::aspects::{DefsAspect, ExecutionAspect, SerdeAspect},
    value::Value,
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

impl<'a, T> Debug for DebugViaDisplay<'a, T>
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

impl<'a> Display for Literal<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> Debug for Literal<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// best-effort formatting of a value
pub fn format_value(
    value: &Value,
    ontology: &(impl AsRef<DefsAspect> + AsRef<SerdeAspect>),
) -> String {
    let defs: &DefsAspect = ontology.as_ref();

    let def = defs.def(value.type_def_id());
    if let Some(operator_addr) = def.operator_addr {
        // TODO: Easier way to report values in "human readable"/JSON format

        let dummy_execution = ExecutionAspect::empty();
        let ctx = crate::interface::serde::OntologyCtx {
            serde: ontology.as_ref(),
            defs: ontology.as_ref(),
            execution: &dummy_execution,
        };
        let processor = SerdeProcessor::new(operator_addr, ProcessorMode::Read, &ctx);

        let mut buf: Vec<u8> = vec![];
        processor
            .serialize_attr(
                AttrRef::Unit(value),
                &mut serde_json::Serializer::new(&mut buf),
            )
            .unwrap();
        String::from(std::str::from_utf8(&buf).unwrap())
    } else {
        "N/A".to_string()
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
