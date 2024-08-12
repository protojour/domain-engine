use std::{fmt::Display, sync::Arc};

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

#[derive(Clone)]
pub enum Stmt<'d> {
    Select(Select<'d>),
}

#[derive(Clone, Default)]
pub struct Select<'d> {
    pub with: Option<With<'d>>,
    pub expressions: Expressions<'d>,
    pub from: Vec<FromItem<'d>>,
    pub where_: Option<Expr<'d>>,
    pub limit: Limit,
}

#[derive(Clone, Default)]
pub struct Expressions<'d> {
    pub items: Vec<Expr<'d>>,
    pub multiline: bool,
}

#[derive(Clone)]
pub struct With<'d> {
    pub recursive: bool,
    pub queries: Vec<WithQuery<'d>>,
}

#[derive(Clone)]
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

#[derive(Clone, Default)]
pub struct Limit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone)]
pub enum Expr<'d> {
    /// path in current scope
    Path(SmallVec<PathSegment<'d>, 2>),
    /// input parameter
    Param(Param),
    Paren(Box<Expr<'d>>),
    LiteralInt(i32),
    Select(Box<Select<'d>>),
    /// a UNION b
    #[allow(unused)]
    Union(Box<Union<'d>>),
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
    Limit(Box<Expr<'d>>, Limit),
    Arc(Arc<Expr<'d>>),
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

    pub fn paren(expr: impl Into<Self>) -> Self {
        Self::Paren(Box::new(expr.into()))
    }

    pub fn array(expr: impl Into<Self>) -> Self {
        Self::Array(Box::new(expr.into()))
    }

    pub fn eq(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::Eq(Box::new(a.into()), Box::new(b.into()))
    }

    pub fn arc(expr: impl Into<Self>) -> Self {
        Self::Arc(Arc::new(expr.into()))
    }
}

#[derive(Clone)]
pub enum Name {
    Alias(Alias),
}

#[derive(Clone)]
pub enum PathSegment<'d> {
    Ident(&'d str),
    Alias(Alias),
    Param(Param),
    /// * (every column)
    Asterisk,
}

#[derive(Clone)]
pub enum FromItem<'d> {
    TableName(TableName<'d>),
    TableNameAs(TableName<'d>, Name),
    Alias(Alias),
    Join(Box<Join<'d>>),
}

#[derive(Clone)]
pub struct Join<'d> {
    pub first: FromItem<'d>,
    pub second: FromItem<'d>,
    pub on: Expr<'d>,
}

#[derive(Clone)]
pub struct Union<'d> {
    pub first: Expr<'d>,
    /// UNION ALL?
    pub all: bool,
    pub second: Expr<'d>,
}

#[derive(Clone)]
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
            expressions = self.expressions,
            from = self.from.iter().format(","),
        )?;

        if let Some(condition) = &self.where_ {
            write!(f, " WHERE {condition}")?;
        }

        write!(f, "{}", self.limit)
    }
}

impl<'d> Display for Expressions<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.multiline {
            writeln!(f, "{}", self.items.iter().format(",\n"))
        } else {
            write!(f, "{}", self.items.iter().format(","))
        }
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            Self::Paren(expr) => write!(f, "({expr})"),
            Self::LiteralInt(i) => write!(f, "{i}"),
            Self::Select(select) => write!(f, "{select}"),
            Self::Union(union) => write!(f, "{union}"),
            Self::And(clauses) => write!(f, "{}", clauses.iter().format(" AND ")),
            Self::Eq(a, b) => write!(f, "{a} = {b}"),
            Self::Row(fields) => write!(f, "ROW({})", fields.iter().format(",")),
            Self::Array(expr) => write!(f, "ARRAY({expr})"),
            Self::ArrayAgg(expr) => write!(f, "ARRAY_AGG({expr})"),
            Self::AsIndex(expr, index) => write!(f, "{expr} AS {index}"),
            Self::CountStarOver => write!(f, "COUNT(*) OVER()"),
            Self::Limit(expr, limit) => write!(f, "{expr}{limit}"),
            Self::Arc(expr) => write!(f, "{expr}"),
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

impl<'d> Display for Union<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first = &self.first;
        let second = &self.second;
        let infix = if self.all { "UNION ALL" } else { "UNION" };

        write!(f, "{first} {infix} {second}")
    }
}

impl<'d> std::convert::From<Select<'d>> for Expr<'d> {
    fn from(value: Select<'d>) -> Self {
        Self::Select(Box::new(value))
    }
}

impl<'d> std::convert::From<Vec<WithQuery<'d>>> for With<'d> {
    fn from(value: Vec<WithQuery<'d>>) -> Self {
        let recursive = value.iter().any(|wq| !wq.column_names.is_empty());
        With {
            recursive,
            queries: value,
        }
    }
}

impl<'d> std::convert::From<Join<'d>> for FromItem<'d> {
    fn from(value: Join<'d>) -> Self {
        FromItem::Join(Box::new(value))
    }
}

impl<'d> std::convert::From<TableName<'d>> for FromItem<'d> {
    fn from(value: TableName<'d>) -> Self {
        Self::TableName(value)
    }
}

impl<'d> std::convert::From<&'d str> for PathSegment<'d> {
    fn from(value: &'d str) -> Self {
        Self::Ident(value)
    }
}

impl<'d> std::convert::From<Alias> for PathSegment<'d> {
    fn from(value: Alias) -> Self {
        Self::Alias(value)
    }
}

impl<'d> std::convert::From<Param> for PathSegment<'d> {
    fn from(value: Param) -> Self {
        Self::Param(value)
    }
}

impl<'d> Display for TableName<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", Ident(self.0), Ident(self.1))
    }
}

#[derive(Clone)]
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
