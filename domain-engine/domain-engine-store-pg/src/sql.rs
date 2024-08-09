use std::fmt::Display;

use itertools::Itertools;
use smallvec::{smallvec, SmallVec};

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

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Alias(pub usize);

impl Alias {
    pub fn incr(&mut self) -> Alias {
        self.0 += 1;
        Self(self.0)
    }
}

impl Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a{}", self.0)
    }
}

pub enum Stmt<'d> {
    Select(Select<'d>),
}

#[derive(Default)]
pub struct Select<'d> {
    pub with: Option<With<'d>>,
    pub expressions: Vec<Expr<'d>>,
    pub from: Vec<FromItem<'d>>,
    pub where_: Option<Expr<'d>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub struct With<'d> {
    pub recursive: bool,
    pub queries: Vec<WithQuery<'d>>,
}

pub struct WithQuery<'d> {
    pub name: Name,
    pub column_names: Vec<&'d str>,
    pub stmt: Stmt<'d>,
}

pub struct Insert<'d> {
    pub into: TableName<'d>,
    pub column_names: Vec<&'d str>,
    pub on_conflict: Option<OnConflict<'d>>,
    pub returning: Vec<Expr<'d>>,
}

pub struct OnConflict<'d> {
    pub target: Option<ConflictTarget<'d>>,
    pub action: ConflictAction<'d>,
}

pub enum ConflictTarget<'d> {
    Columns(Vec<&'d str>),
}

pub enum ConflictAction<'d> {
    DoUpdateSet(Vec<UpdateColumn<'d>>),
    #[allow(unused)]
    DoNothing,
}

pub struct Delete<'d> {
    pub from: TableName<'d>,
    pub where_: Option<Expr<'d>>,
    pub returning: Vec<Expr<'d>>,
}

/// column = expr
pub struct UpdateColumn<'d>(pub &'d str, pub Expr<'d>);

pub enum Expr<'d> {
    /// path in current scope
    Path(SmallVec<PathSegment<'d>, 2>),
    /// input parameter
    Param(Param),
    LiteralInt(i32),
    Select(Box<Select<'d>>),
    /// a AND b
    And(Vec<Expr<'d>>),
    /// a = b
    Eq(Box<Expr<'d>>, Box<Expr<'d>>),
    Row(Vec<Expr<'d>>),
    Array(Box<Expr<'d>>),
    #[allow(unused)]
    ArrayAgg(Box<Expr<'d>>),
    #[allow(unused)]
    AsIndex(Box<Expr<'d>>, Alias),
    /// count(*) over ()
    CountStarOver,
}

pub enum Name {
    Alias(Alias),
}

impl<'d> Expr<'d> {
    pub fn path1(segment: impl Into<PathSegment<'d>>) -> Self {
        Self::Path(smallvec!(segment.into()))
    }

    pub fn path2(a: impl Into<PathSegment<'d>>, b: impl Into<PathSegment<'d>>) -> Self {
        Self::Path(smallvec!(a.into(), b.into()))
    }

    pub fn param(p: usize) -> Self {
        Self::Param(Param(p))
    }

    pub fn array(expr: Self) -> Self {
        Self::Array(Box::new(expr))
    }

    pub fn eq(a: Self, b: Self) -> Self {
        Self::Eq(Box::new(a), Box::new(b))
    }
}

pub enum PathSegment<'d> {
    Ident(&'d str),
    Alias(Alias),
    Param(Param),
    /// * (every column)
    Asterisk,
}

pub enum FromItem<'d> {
    TableName(TableName<'d>),
    TableNameAs(TableName<'d>, Name),
    Alias(Alias),
    Join(Box<Join<'d>>),
}

pub struct Join<'d> {
    pub first: FromItem<'d>,
    pub second: FromItem<'d>,
    pub on: Expr<'d>,
}

pub struct TableName<'d>(pub &'d str, pub &'d str);

impl<'d> TableName<'d> {
    pub fn as_(self, alias: Alias) -> FromItem<'d> {
        FromItem::TableNameAs(self, Name::Alias(alias))
    }
}

impl<'d> Display for Stmt<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Select(select) => write!(f, "{select}"),
        }
    }
}

impl<'d> Display for Select<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(with) = &self.with {
            writeln!(f, "{with}")?;
        }

        write!(
            f,
            "SELECT {expressions} FROM {from}",
            expressions = self.expressions.iter().format(","),
            from = self.from.iter().format(","),
        )?;

        if let Some(condition) = &self.where_ {
            write!(f, " WHERE {condition}")?;
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }

        Ok(())
    }
}

impl<'d> Display for With<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.recursive {
            writeln!(f, "WITH RECURSIVE")?;
        } else {
            writeln!(f, "WITH")?;
        }

        write!(f, "{}", self.queries.iter().format(",\n"))
    }
}

impl<'d> Display for WithQuery<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{name}", name = self.name)?;

        if !self.column_names.is_empty() {
            write!(f, " ({})", self.column_names.iter().format(","))?;
        }

        write!(f, " AS ({stmt})", stmt = self.stmt)
    }
}

impl<'d> Display for Insert<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "INSERT INTO {table_name} ({columns}) VALUES ({values})",
            table_name = self.into,
            columns = self.column_names.iter().map(Ident).format(","),
            values = (0..self.column_names.len()).map(Param).format(","),
        )?;

        if let Some(on_conflict) = &self.on_conflict {
            write!(f, " ON CONFLICT {on_conflict}")?;
        }

        if !self.returning.is_empty() {
            write!(f, " RETURNING {}", self.returning.iter().format(","))?;
        }

        Ok(())
    }
}

impl<'d> Display for OnConflict<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(target) = &self.target {
            write!(f, "{target} ")?;
        }

        write!(f, "{}", self.action)
    }
}

impl<'d> Display for ConflictTarget<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Columns(columns) => write!(f, "({})", columns.iter().map(Ident).format(",")),
        }
    }
}

impl<'d> Display for ConflictAction<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DoUpdateSet(update_columns) => {
                write!(
                    f,
                    "DO UPDATE SET {update_columns}",
                    update_columns = update_columns.iter().format(",")
                )
            }
            Self::DoNothing => {
                write!(f, "DO NOTHING")
            }
        }
    }
}

impl<'d> Display for UpdateColumn<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{column_name} = {expr}",
            column_name = Ident(self.0),
            expr = self.1
        )
    }
}

impl<'d> Display for Delete<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {from}", from = self.from)?;

        if let Some(condition) = &self.where_ {
            write!(f, " WHERE {condition}")?;
        }

        if !self.returning.is_empty() {
            write!(f, " RETURNING {}", self.returning.iter().format(","))?;
        }

        Ok(())
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alias(alias) => write!(f, "{alias}"),
        }
    }
}

impl<'d> Display for Expr<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path(segments) => write!(f, "{}", segments.iter().format(".")),
            Self::Param(param) => write!(f, "{param}"),
            Self::LiteralInt(i) => write!(f, "{i}"),
            Self::And(clauses) => write!(f, "({})", clauses.iter().format(" AND ")),
            Self::Eq(a, b) => write!(f, "{a} = {b}"),
            Self::Select(select) => write!(f, "({select})"),
            Self::Row(fields) => write!(f, "ROW({})", fields.iter().format(",")),
            Self::Array(expr) => write!(f, "ARRAY({expr})"),
            Self::ArrayAgg(expr) => write!(f, "ARRAY_AGG({expr})"),
            Self::AsIndex(expr, index) => write!(f, "{expr} AS {index}"),
            Self::CountStarOver => write!(f, "COUNT(*) OVER()"),
        }
    }
}

impl<'d> Display for PathSegment<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathSegment::Ident(i) => write!(f, "{}", Ident(i)),
            PathSegment::Alias(a) => write!(f, "{a}"),
            PathSegment::Param(p) => write!(f, "{p}"),
            PathSegment::Asterisk => write!(f, "*"),
        }
    }
}

impl<'d> Display for FromItem<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableName(tn) => write!(f, "{tn}"),
            Self::TableNameAs(tn, alias) => write!(f, "{tn} AS {alias}"),
            Self::Alias(alias) => write!(f, "{alias}"),
            Self::Join(join) => write!(f, "{join}"),
        }
    }
}

impl<'d> Display for Join<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first = &self.first;
        let second = &self.second;
        let on = &self.on;

        write!(f, "{first} JOIN {second} ON {on}")
    }
}

impl<'d> From<Select<'d>> for Expr<'d> {
    fn from(value: Select<'d>) -> Self {
        Self::Select(Box::new(value))
    }
}

impl<'d> From<Vec<WithQuery<'d>>> for With<'d> {
    fn from(value: Vec<WithQuery<'d>>) -> Self {
        let recursive = value.iter().any(|wq| !wq.column_names.is_empty());
        With {
            recursive,
            queries: value,
        }
    }
}

impl<'d> From<Join<'d>> for FromItem<'d> {
    fn from(value: Join<'d>) -> Self {
        FromItem::Join(Box::new(value))
    }
}

impl<'d> From<TableName<'d>> for FromItem<'d> {
    fn from(value: TableName<'d>) -> Self {
        Self::TableName(value)
    }
}

impl<'d> From<&'d str> for PathSegment<'d> {
    fn from(value: &'d str) -> Self {
        Self::Ident(value)
    }
}

impl<'d> From<Alias> for PathSegment<'d> {
    fn from(value: Alias) -> Self {
        Self::Alias(value)
    }
}

impl<'d> From<Param> for PathSegment<'d> {
    fn from(value: Param) -> Self {
        Self::Param(value)
    }
}

impl<'d> Display for TableName<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", Ident(self.0), Ident(self.1))
    }
}
pub struct Param(pub usize);

impl Display for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.0 + 1)
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
