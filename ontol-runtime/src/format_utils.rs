use std::fmt::Display;

pub(super) struct Missing<T> {
    pub items: Vec<T>,
    pub logic_op: LogicOp,
}

impl<T: Display> Display for Missing<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.items.as_slice() {
            [] => panic!("BUG: Nothing is missing!"),
            [single] => write!(f, "{}", single),
            [a, b] => write!(f, "{} {} {}", a, self.logic_op.name(), b),
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
            write!(f, "{}", next)?;
            if iterator.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

/// Macro for formatting into a SmartString instead of a std::String
#[macro_export]
macro_rules! smart_format {
    ($fmt:expr $(,$args:expr)*) => {{
        use std::fmt::Write;
        let mut buf = ::smartstring::alias::String::new();
        write!(&mut buf, $fmt $(,$args)*).unwrap();
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
