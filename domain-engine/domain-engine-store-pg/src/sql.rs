use std::{fmt::Display, sync::Arc};

use itertools::Itertools;
use ontol_runtime::query::order::Direction;
use smallvec::{SmallVec, smallvec};

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
pub enum Stmt<'a> {
    Select(Select<'a>),
}

#[derive(Clone, Default)]
pub struct Select<'a> {
    pub with: Option<With<'a>>,
    pub expressions: Expressions<'a>,
    pub from: Vec<FromItem<'a>>,
    pub where_: Option<Expr<'a>>,
    pub order_by: OrderBy<'a>,
    pub limit: Limit,
}

pub struct Insert<'a> {
    pub with: Option<With<'a>>,
    pub into: TableName<'a>,
    pub as_: Option<Alias>,
    pub column_names: Vec<&'a str>,
    pub values: Vec<Expr<'a>>,
    pub on_conflict: Option<OnConflict<'a>>,
    pub returning: Vec<Expr<'a>>,
}

pub struct Update<'a> {
    pub with: Option<With<'a>>,
    pub table_name: TableName<'a>,
    pub set: Vec<UpdateColumn<'a>>,
    pub where_: Option<Expr<'a>>,
    pub returning: Vec<Expr<'a>>,
}

pub struct Delete<'a> {
    pub from: TableName<'a>,
    pub where_: Option<Expr<'a>>,
    pub returning: Vec<Expr<'a>>,
}

pub struct Vacuum<'a> {
    pub tables: Vec<TableName<'a>>,
}

impl<'a> WhereExt<'a> for Select<'a> {
    fn where_mut(&mut self) -> &mut Option<Expr<'a>> {
        &mut self.where_
    }
}

impl<'a> WhereExt<'a> for Update<'a> {
    fn where_mut(&mut self) -> &mut Option<Expr<'a>> {
        &mut self.where_
    }
}

impl<'a> WhereExt<'a> for Delete<'a> {
    fn where_mut(&mut self) -> &mut Option<Expr<'a>> {
        &mut self.where_
    }
}

#[derive(Clone, Default)]
pub struct Expressions<'a> {
    pub items: Vec<Expr<'a>>,
    pub multiline: bool,
}

impl<'a> From<Vec<Expr<'a>>> for Expressions<'a> {
    fn from(value: Vec<Expr<'a>>) -> Self {
        Self {
            items: value,
            multiline: false,
        }
    }
}

#[derive(Clone)]
pub struct With<'a> {
    pub recursive: bool,
    pub queries: Vec<WithQuery<'a>>,
}

#[derive(Clone)]
pub struct WithQuery<'a> {
    pub name: Name,
    pub column_names: Vec<&'a str>,
    pub stmt: Stmt<'a>,
}

pub struct OnConflict<'a> {
    pub target: Option<ConflictTarget<'a>>,
    pub action: ConflictAction<'a>,
}

pub enum ConflictTarget<'a> {
    Columns(Vec<&'a str>),
}

pub enum ConflictAction<'a> {
    DoUpdateSet(Vec<UpdateColumn<'a>>),
    DoNothing,
}

/// column = expr
pub struct UpdateColumn<'a>(pub &'a str, pub Expr<'a>);

#[derive(Clone, Default)]
pub struct OrderBy<'a> {
    pub expressions: Vec<OrderByExpr<'a>>,
}

#[derive(Clone)]
pub struct OrderByExpr<'a>(pub Expr<'a>, pub Direction);

impl<'a> From<Expr<'a>> for OrderByExpr<'a> {
    fn from(value: Expr<'a>) -> Self {
        OrderByExpr(value, Direction::Ascending)
    }
}

#[derive(Clone, Default)]
pub struct Limit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone)]
pub enum Expr<'a> {
    /// path in current scope
    Path(Path<'a>),
    /// input parameter
    Param(Param),
    Default,
    LiteralInt(i32),
    LiteralStr(&'a str),
    LiteralBytea(&'a [u8]),
    /// (a, b, ..)
    Tuple(Vec<Expr<'a>>),
    /// (a)
    Paren(Box<Expr<'a>>),
    Select(Box<Select<'a>>),
    /// a UNION b
    Union(Box<Union<'a>>),
    /// a AND b
    And(Vec<Expr<'a>>),
    Or(Vec<Expr<'a>>),
    /// a = b
    Eq(Box<Expr<'a>>, Box<Expr<'a>>),
    /// a < b
    Lt(Box<Expr<'a>>, Box<Expr<'a>>),
    /// a <= b
    Lte(Box<Expr<'a>>, Box<Expr<'a>>),
    /// a > b
    Gt(Box<Expr<'a>>, Box<Expr<'a>>),
    /// a >= b
    Gte(Box<Expr<'a>>, Box<Expr<'a>>),
    /// a IN b
    In(Box<Expr<'a>>, Box<Expr<'a>>),
    Exists(Box<Expr<'a>>),
    Row(Vec<Expr<'a>>),
    Any(Box<Expr<'a>>),
    Array(Box<Expr<'a>>),
    ArrayAgg(Box<Expr<'a>>),
    /// value, separator
    StringAgg(Box<Expr<'a>>, Box<Expr<'a>>),
    AsIndex(Box<Expr<'a>>, Alias),
    /// count(*) over ()
    CountStarOver,
    Limit(Box<Expr<'a>>, Limit),
    Arc(Arc<Expr<'a>>),
}

impl<'a> Expr<'a> {
    pub fn path1(segment: impl Into<PathSegment<'a>>) -> Self {
        Self::Path(Path(smallvec!(segment.into())))
    }

    pub fn path2(a: impl Into<PathSegment<'a>>, b: impl Into<PathSegment<'a>>) -> Self {
        Self::Path(Path(smallvec!(a.into(), b.into())))
    }

    pub fn param(p: usize) -> Self {
        Self::Param(Param(p))
    }

    pub fn paren(expr: impl Into<Self>) -> Self {
        Self::Paren(Box::new(expr.into()))
    }

    pub fn select(select: Select<'a>) -> Self {
        Self::Select(Box::new(select))
    }

    pub fn array(expr: impl Into<Self>) -> Self {
        Self::Array(Box::new(expr.into()))
    }

    pub fn eq(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::Eq(Box::new(a.into()), Box::new(b.into()))
    }

    pub fn less_than(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::Lt(Box::new(a.into()), Box::new(b.into()))
    }

    pub fn in_(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::In(Box::new(a.into()), Box::new(b.into()))
    }

    pub fn arc(expr: impl Into<Self>) -> Self {
        Self::Arc(Arc::new(expr.into()))
    }
}

#[derive(Clone)]
pub struct Path<'a>(SmallVec<PathSegment<'a>, 2>);

impl<'a> Path<'a> {
    pub fn empty() -> Self {
        Self(smallvec![])
    }

    pub fn join(&self, segment: impl Into<PathSegment<'a>>) -> Self {
        let mut segments = self.0.clone();
        segments.push(segment.into());
        Self(segments)
    }
}

impl<'a> FromIterator<PathSegment<'a>> for Path<'a> {
    fn from_iter<T: IntoIterator<Item = PathSegment<'a>>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<'a> From<PathSegment<'a>> for Path<'a> {
    fn from(value: PathSegment<'a>) -> Self {
        Self(smallvec![value])
    }
}

#[derive(Clone)]
pub enum Name {
    Alias(Alias),
}

impl From<Alias> for Name {
    fn from(value: Alias) -> Self {
        Self::Alias(value)
    }
}

#[derive(Clone)]
pub enum PathSegment<'a> {
    Ident(&'a str),
    Alias(Alias),
    Param(Param),
    /// * (every column)
    Asterisk,
}

#[derive(Clone)]
pub enum FromItem<'a> {
    TableName(TableName<'a>),
    TableNameAs(TableName<'a>, Name),
    Alias(Alias),
    Join(Box<Join<'a>>),
    Select(Box<Select<'a>>),
}

#[derive(Clone)]
pub struct Join<'a> {
    pub first: FromItem<'a>,
    pub second: FromItem<'a>,
    pub on: Expr<'a>,
}

#[derive(Clone)]
pub struct Union<'a> {
    pub first: Expr<'a>,
    /// UNION ALL?
    pub all: bool,
    pub second: Expr<'a>,
}

#[derive(Clone)]
pub struct TableName<'a>(pub &'a str, pub &'a str);

impl<'a> TableName<'a> {
    pub fn as_(self, alias: Alias) -> FromItem<'a> {
        FromItem::TableNameAs(self, alias.into())
    }
}

impl Display for Stmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Select(select) => write!(f, "{select}"),
        }
    }
}

impl Display for Select<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(with) = &self.with {
            writeln!(f, "{with}")?;
        }

        write!(f, "SELECT")?;

        if !self.expressions.items.is_empty() {
            write!(f, " {e}", e = self.expressions)?;
        }

        write!(f, " FROM {}", self.from.iter().format(","))?;

        if let Some(condition) = &self.where_ {
            write!(f, " WHERE {condition}")?;
        }

        write!(f, "{}", self.order_by)?;
        write!(f, "{}", self.limit)
    }
}

impl Display for Insert<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(with) = &self.with {
            writeln!(f, "{with}")?;
        }

        write!(f, "INSERT INTO {table_name}", table_name = self.into,)?;

        if let Some(as_) = self.as_ {
            write!(f, " AS {as_}")?;
        }

        write!(
            f,
            " ({columns}) VALUES ({values})",
            columns = self.column_names.iter().map(Ident).format(","),
            values = self.values.iter().format(","),
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

impl Display for Update<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(with) = &self.with {
            writeln!(f, "{with}")?;
        }

        write!(
            f,
            "UPDATE {table_name} SET {set}",
            table_name = self.table_name,
            set = self.set.iter().format(", ")
        )?;

        if let Some(condition) = &self.where_ {
            write!(f, " WHERE {condition}")?;
        }

        if !self.returning.is_empty() {
            write!(f, " RETURNING {}", self.returning.iter().format(","))?;
        }

        Ok(())
    }
}

impl Display for Delete<'_> {
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

impl Display for Vacuum<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VACUUM {tables}",
            tables = self.tables.iter().format(", ")
        )?;
        Ok(())
    }
}

impl Display for Expressions<'_> {
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

impl Display for OrderBy<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.expressions.is_empty() {
            return Ok(());
        }

        write!(f, " ORDER BY {}", self.expressions.iter().format(","))
    }
}

impl Display for OrderByExpr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;

        if matches!(self.1, Direction::Descending) {
            write!(f, " DESC")?;
        }

        Ok(())
    }
}

impl Display for With<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.recursive {
            writeln!(f, "WITH RECURSIVE")?;
        } else {
            writeln!(f, "WITH")?;
        }

        write!(f, "{}", self.queries.iter().format(",\n"))
    }
}

impl Display for WithQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{name}", name = self.name)?;

        if !self.column_names.is_empty() {
            write!(f, " ({})", self.column_names.iter().format(","))?;
        }

        write!(f, " AS ({stmt})", stmt = self.stmt)
    }
}

impl Display for OnConflict<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(target) = &self.target {
            write!(f, "{target} ")?;
        }

        write!(f, "{}", self.action)
    }
}

impl Display for ConflictTarget<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Columns(columns) => write!(f, "({})", columns.iter().map(Ident).format(",")),
        }
    }
}

impl Display for ConflictAction<'_> {
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

impl Display for UpdateColumn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{column_name}={expr}",
            column_name = Ident(self.0),
            expr = self.1
        )
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alias(alias) => write!(f, "{alias}"),
        }
    }
}

impl Display for Expr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path(path) => write!(f, "{path}"),
            Self::Param(param) => write!(f, "{param}"),
            Self::Default => write!(f, "DEFAULT"),
            Self::LiteralInt(i) => write!(f, "{i}"),
            Self::LiteralStr(s) => write!(f, "'{s}'"),
            Self::LiteralBytea(b) => write!(f, "'\\x{}'", HexSlice(b)),
            Self::Tuple(t) => write!(f, "({})", t.iter().format(",")),
            Self::Paren(expr) => write!(f, "({expr})"),
            Self::Select(select) => write!(f, "({select})"),
            Self::Union(union) => write!(f, "{union}"),
            Self::And(clauses) => write!(f, "{}", clauses.iter().format(" AND ")),
            Self::Or(clauses) => write!(f, "{}", clauses.iter().format(" OR ")),
            Self::Eq(a, b) => write!(f, "{a}={b}"),
            Self::Lt(a, b) => write!(f, "{a}<{b}"),
            Self::Lte(a, b) => write!(f, "{a}<={b}"),
            Self::Gt(a, b) => write!(f, "{a}>{b}"),
            Self::Gte(a, b) => write!(f, "{a}>={b}"),
            Self::In(a, b) => write!(f, "{a} IN {b}"),
            Self::Exists(expr) => write!(f, "EXISTS({expr})"),
            Self::Row(fields) => write!(f, "ROW({})", fields.iter().format(",")),
            Self::Any(expr) => write!(f, "ANY({expr})"),
            Self::Array(expr) => write!(f, "ARRAY({expr})"),
            Self::ArrayAgg(expr) => write!(f, "ARRAY_AGG({expr})"),
            Self::StringAgg(expr, sep) => write!(f, "STRING_AGG({expr}, {sep})"),
            Self::AsIndex(expr, index) => write!(f, "{expr} AS {index}"),
            Self::CountStarOver => write!(f, "COUNT(*) OVER()"),
            Self::Limit(expr, limit) => write!(f, "{expr}{limit}"),
            Self::Arc(expr) => write!(f, "{expr}"),
        }
    }
}

impl Display for Path<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.iter().format("."))
    }
}

impl Display for PathSegment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathSegment::Ident(i) => write!(f, "{}", Ident(i)),
            PathSegment::Alias(a) => write!(f, "{a}"),
            PathSegment::Param(p) => write!(f, "{p}"),
            PathSegment::Asterisk => write!(f, "*"),
        }
    }
}

impl Display for FromItem<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableName(tn) => write!(f, "{tn}"),
            Self::TableNameAs(tn, alias) => write!(f, "{tn} AS {alias}"),
            Self::Alias(alias) => write!(f, "{alias}"),
            Self::Join(join) => write!(f, "{join}"),
            Self::Select(select) => write!(f, "({select})"),
        }
    }
}

impl Display for Join<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first = &self.first;
        let second = &self.second;
        let on = &self.on;

        write!(f, "{first} JOIN {second} ON {on}")
    }
}

impl Display for Union<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first = &self.first;
        let second = &self.second;
        let infix = if self.all { "UNION ALL" } else { "UNION" };

        write!(f, "{first} {infix} {second}")
    }
}

impl<'a> std::convert::From<Select<'a>> for Expr<'a> {
    fn from(value: Select<'a>) -> Self {
        Self::Select(Box::new(value))
    }
}

impl<'a> std::convert::From<Path<'a>> for Expr<'a> {
    fn from(value: Path<'a>) -> Self {
        Self::Path(value)
    }
}

impl<'a> std::convert::From<Vec<WithQuery<'a>>> for With<'a> {
    fn from(value: Vec<WithQuery<'a>>) -> Self {
        let recursive = value.iter().any(|wq| !wq.column_names.is_empty());
        With {
            recursive,
            queries: value,
        }
    }
}

impl<'a> std::convert::From<Join<'a>> for FromItem<'a> {
    fn from(value: Join<'a>) -> Self {
        FromItem::Join(Box::new(value))
    }
}

impl<'a> std::convert::From<TableName<'a>> for FromItem<'a> {
    fn from(value: TableName<'a>) -> Self {
        Self::TableName(value)
    }
}

impl<'a> std::convert::From<&'a str> for PathSegment<'a> {
    fn from(value: &'a str) -> Self {
        Self::Ident(value)
    }
}

impl std::convert::From<Alias> for PathSegment<'_> {
    fn from(value: Alias) -> Self {
        Self::Alias(value)
    }
}

impl std::convert::From<Param> for PathSegment<'_> {
    fn from(value: Param) -> Self {
        Self::Param(value)
    }
}

impl Display for TableName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", Ident(self.0), Ident(self.1))
    }
}

pub trait WhereExt<'a> {
    fn where_mut(&mut self) -> &mut Option<Expr<'a>>;

    fn where_and(&mut self, expr: Expr<'a>) {
        match self.where_mut().take() {
            Some(Expr::And(mut clauses)) => {
                clauses.push(expr);
                (*self.where_mut()) = Some(Expr::And(clauses));
            }
            Some(old) => {
                (*self.where_mut()) = Some(Expr::And(vec![old, expr]));
            }
            None => {
                (*self.where_mut()) = Some(expr);
            }
        }
    }
}

#[derive(Clone)]
pub struct Param(pub usize);

impl Display for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.0 + 1)
    }
}

struct HexSlice<'a>(&'a [u8]);

impl Display for HexSlice<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{:X} ", byte)?;
        }
        Ok(())
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
