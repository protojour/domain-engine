use std::fmt::Display;

use itertools::Itertools;
use tokio_postgres::{types::FromSql, Row};

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

pub struct Insert<'d> {
    pub schema: &'d str,
    pub table: &'d str,
    pub columns: Vec<&'d str>,
    pub returning: Vec<&'d str>,
}

impl<'d> Display for Insert<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "INSERT INTO {schema}.{table} ({columns}) VALUES ({binds})",
            schema = EscapeIdent(self.schema),
            table = EscapeIdent(self.table),
            columns = self.columns.iter().map(EscapeIdent).format(","),
            binds = self.columns.iter().enumerate().map(Dollar).format(","),
        )?;

        if !self.returning.is_empty() {
            write!(
                f,
                " RETURNING {}",
                self.returning.iter().map(EscapeIdent).format(",")
            )?;
        }

        Ok(())
    }
}

struct Dollar<T>((usize, T));

impl<T> Display for Dollar<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.0 .0 + 1)
    }
}

pub trait Unpack<'a, T> {
    fn unpack(&'a self) -> T;
}

impl<'a, T> Unpack<'a, (T,)> for Row
where
    T: FromSql<'a>,
{
    fn unpack(&'a self) -> (T,) {
        (self.get(0),)
    }
}

impl<'a, T0, T1> Unpack<'a, (T0, T1)> for Row
where
    T0: FromSql<'a>,
    T1: FromSql<'a>,
{
    fn unpack(&'a self) -> (T0, T1) {
        (self.get(0), self.get(1))
    }
}

impl<'a, T0, T1, T2> Unpack<'a, (T0, T1, T2)> for Row
where
    T0: FromSql<'a>,
    T1: FromSql<'a>,
    T2: FromSql<'a>,
{
    fn unpack(&'a self) -> (T0, T1, T2) {
        (self.get(0), self.get(1), self.get(2))
    }
}

impl<'a, T0, T1, T2, T3> Unpack<'a, (T0, T1, T2, T3)> for Row
where
    T0: FromSql<'a>,
    T1: FromSql<'a>,
    T2: FromSql<'a>,
    T3: FromSql<'a>,
{
    fn unpack(&'a self) -> (T0, T1, T2, T3) {
        (self.get(0), self.get(1), self.get(2), self.get(3))
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
