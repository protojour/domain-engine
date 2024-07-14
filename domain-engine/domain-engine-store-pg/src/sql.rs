use std::fmt::Display;

pub struct EscapeIdent<T>(pub T);

impl<T: AsRef<str>> Display for EscapeIdent<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"")?;

        let str = self.0.as_ref();
        let mut prev = 0;

        for (index, ch) in str.char_indices() {
            if ch == '"' {
                write!(f, "{}", &str[prev..index])?;
                prev = index;

                // a standalone '"' is escaped by inserting another '"':
                write!(f, "\"")?;
            }
        }

        write!(f, "{}\"", &str[prev..str.len()])
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::EscapeIdent;

    #[track_caller]
    fn test_escape_identifier(input: &str, expected: &str) {
        assert_eq!(
            expected,
            format!("{}", EscapeIdent(input)),
            "fmt implementation should equal expected"
        );
        assert_eq!(
            expected,
            postgres_protocol::escape::escape_identifier(input),
            "postgres_protocol implementation should equal expected"
        );
    }

    #[test]
    fn escape_identifier() {
        test_escape_identifier(r#"trivial"#, r#""trivial""#);
        test_escape_identifier(r#"\backslash\\"#, r#""\backslash\\""#);
        test_escape_identifier(r#"quote:" escaped:\""#, r#""quote:"" escaped:\""""#);
    }
}
