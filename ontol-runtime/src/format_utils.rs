use std::fmt::{Debug, Display};

pub struct Missing<T> {
    pub items: Vec<T>,
    pub logic_op: LogicOp,
}

impl<T: Display> Display for Missing<T> {
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

/// Macro for formatting into a SmartString instead of a std::String
#[macro_export]
macro_rules! smart_format {
    ($($arg:tt)*) => {{
        use std::fmt::Write;
        let mut buf = ::smartstring::alias::String::new();
        buf.write_fmt(std::format_args!($($arg)*)).unwrap();
        buf
    }};
}

#[cfg(test)]
mod tests {
    use smartstring::alias::String;

    #[test]
    fn test_smart_format() {
        let s: String = smart_format!("a {}", 42);
        assert_eq!(s, "a 42");
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

pub fn try_alpha_to_u32(sym: &str) -> Option<u32> {
    if sym.is_empty() {
        return None;
    }

    let mut num: u32 = 0;
    let mut iterator = sym.chars().peekable();

    while let Some(char) = iterator.next() {
        if !char.is_ascii_lowercase() {
            return None;
        }

        let value = u32::from(char) - u32::from('a');
        num += value;

        if iterator.peek().is_some() {
            num = (num + 1) * 26;
        }
    }

    Some(num)
}

pub struct AsAlpha(pub u32);

impl Display for AsAlpha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 >= 26 {
            write!(f, "{}", AsAlpha((self.0 / 26) - 1))?;
        }

        let rem = self.0 % 26;
        write!(f, "{}", char::from_u32(u32::from('a') + rem).unwrap())
    }
}

#[test]
fn test_as_alpha() {
    assert_eq!("a", format!("{}", AsAlpha(0)));
    assert_eq!("z", format!("{}", AsAlpha(25)));
    assert_eq!("aa", format!("{}", AsAlpha(26)));
    assert_eq!("az", format!("{}", AsAlpha(51)));
    assert_eq!("ba", format!("{}", AsAlpha(52)));
    assert_eq!("yq", format!("{}", AsAlpha(666)));
}
