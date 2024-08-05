use std::fmt::Display;

use itertools::Itertools;
use tokio_postgres::{types::FromSql, Row};

pub struct Ident<T>(pub T);

impl<T: AsRef<str>> Display for Ident<T> {
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

pub struct Select<'d> {
    // TODO: `with`
    pub expressions: Vec<Expression<'d>>,
    pub from: Vec<FromItem<'d>>,
    pub limit: Option<usize>,
}

pub struct Insert<'d> {
    pub table_name: TableName<'d>,
    pub column_names: Vec<&'d str>,
    pub returning: Vec<&'d str>,
}

pub enum Expression<'d> {
    Column(&'d str),
    #[allow(unused)]
    Param(Param),
}

pub enum FromItem<'d> {
    TableName(TableName<'d>),
}

pub struct TableName<'d>(pub &'d str, pub &'d str);

impl<'d> Display for Select<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SELECT {expressions} FROM {from}",
            expressions = self.expressions.iter().format(","),
            from = self.from.iter().format(","),
        )?;

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }

        Ok(())
    }
}

impl<'d> Display for Insert<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "INSERT INTO {table_name} ({columns}) VALUES ({values})",
            table_name = self.table_name,
            columns = self.column_names.iter().map(Ident).format(","),
            values = (0..self.column_names.len()).map(Param).format(","),
        )?;

        if !self.returning.is_empty() {
            write!(
                f,
                " RETURNING {}",
                self.returning.iter().map(Ident).format(",")
            )?;
        }

        Ok(())
    }
}

impl<'d> Display for Expression<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(name) => write!(f, "{}", Ident(name)),
            Self::Param(param) => write!(f, "{param}"),
        }
    }
}

impl<'d> Display for FromItem<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableName(tn) => write!(f, "{tn}"),
        }
    }
}

impl<'d> From<TableName<'d>> for FromItem<'d> {
    fn from(value: TableName<'d>) -> Self {
        Self::TableName(value)
    }
}

impl<'d> Display for TableName<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", Ident(self.0), Ident(self.1))
    }
}
pub struct Param(usize);

impl Display for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.0 + 1)
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
    use crate::sql::Ident;

    #[track_caller]
    fn test_escape_ident(input: &str, expected: &str) {
        assert_eq!(
            expected,
            format!("{}", Ident(input)),
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
        test_escape_ident(r#"trivial"#, r#""trivial""#);
        test_escape_ident(r#"\backslash\\"#, r#""\backslash\\""#);
        test_escape_ident(r#"quote:" escaped:\""#, r#""quote:"" escaped:\""""#);
    }
}
