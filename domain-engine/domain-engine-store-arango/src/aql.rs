use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{Display, Formatter, Result},
    ops::Range,
};

use indexmap::IndexSet;
use indoc::writedoc;
use ontol_runtime::ontology::Ontology;

use crate::{data_store::EdgeWriteMode, ArangoDatabase};

/// Helper struct to build AqlQuery
pub struct MetaQuery<'a> {
    /// Set of values for the WITH clause
    pub with: IndexSet<String>,
    /// LET or FOR loop variable name for this (sub)query
    pub var: String,
    /// Optional main selection; DOCUMENT or FOR loop
    pub selection: Option<Selection>,
    /// Array of DOCUMENT operations, always first in a query
    pub docs: Vec<Operation>,
    /// Array of main operations
    pub ops: Vec<Operation>,
    /// Map of data for INSERT and UPDATE by prop name, after main operations
    pub upserts: HashMap<String, MetaQueryData>,
    /// Map of data for edges by prop name, after upserts
    pub rels: HashMap<String, MetaQueryData>,
    /// RETURN expression
    pub return_var: String,
    /// Additional RETURN expression values for MERGE
    pub return_vars: PropMap,
    /// AQL bindVars passed along with query
    pub bind_vars: HashMap<String, serde_json::Value>,
    /// Ontology reference
    pub ontology: &'a Ontology,
    /// Database reference
    pub database: &'a ArangoDatabase,
}

/// Helper struct to build complex queries
#[derive(Default)]
pub struct MetaQueryData {
    /// Optional EdgeWriteMode for this data
    pub mode: Option<EdgeWriteMode>,
    /// Variable name for this data array
    pub var: String,
    /// Optional main selection; DOCUMENT or FOR loop
    pub selection: Option<Selection>,
    /// Optional FILTER _key
    pub filter_key: Option<String>,
    /// Array of serialized JSON data
    pub data: Vec<String>,
    /// RETURN expression values for MERGE
    pub return_vars: PropMap,
    /// Optional Direction for edges
    pub direction: Option<Direction>,
    /// Optional OPTIONS JSON object
    pub options: Option<String>,
}

impl<'a> MetaQuery<'a> {
    /// Initializer from &str
    pub fn from(var: &str, ontology: &'a Ontology, database: &'a ArangoDatabase) -> Self {
        Self {
            with: IndexSet::default(),
            var: var.to_string(),
            selection: None,
            docs: vec![],
            ops: vec![],
            upserts: HashMap::new(),
            rels: HashMap::new(),
            return_var: var.to_string(),
            return_vars: PropMap::default(),
            bind_vars: HashMap::new(),
            ontology,
            database,
        }
    }
}

/// AQL Query
#[derive(Clone, Default, Debug)]
pub struct Query {
    pub with: Option<Vec<String>>,
    pub selection: Option<Selection>,
    pub operations: Option<Vec<Operation>>,
    pub returns: Return,
    pub indent: usize,
}

/// AQL Query selection
#[derive(Clone, Debug)]
pub enum Selection {
    Document(String, String),
    Loop(For),
}

/// AQL Operations
#[derive(Clone, Debug)]
pub enum Operation {
    Selection(Selection),
    Filter(Filter),
    Query(Query),
    Let(Let),
    LetData(LetData),
    Upsert(Upsert),
    Insert(Insert),
    Update(Update),
    Remove(Remove),
    Limit(Limit),
    Sort(Sort),
}

/// AQL LET statement
#[derive(Clone, Default, Debug)]
pub struct Let {
    pub var: String,
    pub query: Query,
}

/// AQL LET data array statement
#[derive(Clone, Default, Debug)]
pub struct LetData {
    pub var: String,
    pub data: Vec<String>,
}

/// AQL FOR loop
#[derive(Clone, Default, Debug)]
pub struct For {
    pub var: String,
    pub edge: Option<String>,
    pub depth: Option<Range<usize>>,
    pub direction: Option<Direction>,
    pub object: String,
    pub edges: Option<Vec<String>>,
}

/// AQL edge traversal direction
#[derive(Clone, Debug)]
pub enum Direction {
    Inbound,
    Outbound,
    Any,
}

/// AQL FILTER statement
#[derive(Clone, Default, Debug)]
pub struct Filter {
    pub var: String,
    pub comp: Option<Comparison>,
    pub val: Option<String>,
}

/// AQL logical operatior
#[derive(Clone, Debug)]
pub enum Logical {
    And,
    Or,
}

/// AQL comparison operator
#[derive(Clone, Debug)]
pub enum Comparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    In,
    NotIn,
    Like,
    NotLike,
    Match,
    NotMatch,
    AllIn,
    NoneIn,
    AnyIn,
}

/// AQL SORT statement
#[derive(Clone, Default, Debug)]
pub struct Sort {
    pub sorts: Vec<SortExpression>,
}

/// AQL SORT statement
#[derive(Clone, Default, Debug)]
pub struct SortExpression {
    pub var: String,
    pub direction: SortDirection,
}

/// AQL sort order
#[derive(Clone, Default, Debug)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// AQL LIMIT statement
#[derive(Clone, Default, Debug)]
pub struct Limit {
    pub skip: usize,
    pub limit: usize,
}

/// AQL UPSERT statement
#[derive(Clone, Default, Debug)]
pub struct Upsert {
    pub search: String,
    pub insert: Insert,
    pub update: Update,
    pub collection: String,
    pub options: Option<String>,
}

/// AQL INSERT statement
#[derive(Clone, Default, Debug)]
pub struct Insert {
    pub data: String,
    pub collection: String,
    pub options: Option<String>,
}

/// AQL UPDATE statement
#[derive(Clone, Default, Debug)]
pub struct Update {
    pub var: Option<String>,
    pub data: String,
    pub collection: String,
    pub options: Option<String>,
}

/// AQL REMOVE statement
#[derive(Clone, Default, Debug)]
pub struct Remove {
    pub key: String,
    pub collection: String,
    pub options: Option<String>,
}

/// AQL RETURN statement
#[derive(Clone, Default, Debug)]
pub struct Return {
    pub distinct: bool,
    pub var: String,
    pub merge: Option<String>,
    pub pre_merge: Option<String>,
    pub post_merge: Option<String>,
}

/// Newtype for e.g. MERGE properties, with Display implementation
#[derive(Default, Debug)]
pub struct PropMap(HashMap<String, Vec<String>>);

impl PropMap {
    pub fn entry(&mut self, key: String) -> Entry<String, Vec<String>> {
        self.0.entry(key)
    }

    pub fn to_string_maybe(&self) -> Option<String> {
        match self.0.len() {
            0 => None,
            _ => Some(format!("{self}")),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut indent = str::repeat("    ", self.indent);

        if let Some(with) = &self.with {
            writeln!(f, "WITH {}", with.join(", "))?;
        }
        if let Some(selection) = &self.selection {
            writeln!(f, "{indent}{}", selection)?;
        }

        indent = if let Some(Selection::Loop(_)) = self.selection {
            str::repeat("    ", self.indent + 1)
        } else {
            indent
        };

        if let Some(operations) = &self.operations {
            for op in operations {
                match op {
                    // so... this is clumsy, but it works...
                    Operation::Let(op) => {
                        let let_indent = match self.selection {
                            Some(Selection::Loop(_)) => 2,
                            _ => 1,
                        };
                        let mut query = op.query.clone();
                        query.indent += self.indent + let_indent;
                        writeln!(f, "{indent}LET {} = (\n{}{indent})", op.var, query)?;
                    }
                    Operation::LetData(op) => {
                        let data = op.data.join(&format!(",\n{indent}    "));
                        writeln!(
                            f,
                            "{indent}LET {} = [\n{indent}    {}\n{indent}]",
                            op.var, data
                        )?;
                    }
                    Operation::Upsert(op) => {
                        writeln!(f, "{indent}UPSERT {}", op.search)?;
                        writeln!(f, "{indent}INSERT {}", op.insert.data)?;
                        writeln!(f, "{indent}UPDATE {}", op.update.data)?;
                        writeln!(f, "{indent}IN {}", op.collection)?;
                    }
                    _ => writeln!(f, "{indent}{op}")?,
                }
            }
        }
        writeln!(f, "{indent}{}", self.returns)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Operation::Selection(op) => write!(f, "{op}"),
            Operation::Filter(op) => write!(f, "{op}"),
            Operation::Query(op) => write!(f, "{op}"),
            Operation::Let(op) => write!(f, "{op}"),
            Operation::LetData(op) => write!(f, "{op}"),
            Operation::Upsert(op) => write!(f, "{op}"),
            Operation::Insert(op) => write!(f, "{op}"),
            Operation::Update(op) => write!(f, "{op}"),
            Operation::Remove(op) => write!(f, "{op}"),
            Operation::Limit(op) => write!(f, "{op}"),
            Operation::Sort(op) => write!(f, "{op}"),
        }
    }
}

impl Display for Selection {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Selection::Document(var, id) => write!(f, "LET {} = DOCUMENT({})", var, id),
            Selection::Loop(sel) => write!(f, "{sel}"),
        }
    }
}

impl Display for Let {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        writedoc! {f, r#"
            LET {} = (
                {}
            )"#, self.var, self.query
        }
    }
}

impl Display for LetData {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        writedoc! {f, r#"
            LET {} = [
                {}
            ]"#, self.var, self.data.join(",\n    ")
        }
    }
}

impl Display for For {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "FOR {}", self.var)?;
        if let Some(edge) = &self.edge {
            write!(f, ", {edge}")?;
        }
        write!(f, " IN")?;
        if let Some(depth) = &self.depth {
            write!(f, " {}..{}", depth.start, depth.end)?;
        }
        if let Some(direction) = &self.direction {
            write!(f, " {direction}")?;
        }
        write!(f, " {}", self.object)?;
        if let Some(edges) = &self.edges {
            write!(f, " {}", edges.join(", "))?;
        }
        Ok(())
    }
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Direction::Inbound => write!(f, "INBOUND"),
            Direction::Outbound => write!(f, "OUTBOUND"),
            Direction::Any => write!(f, "ANY"),
        }
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "FILTER {}", self.var)?;
        if let Some(comp) = &self.comp {
            write!(f, " {comp}")?;
        }
        if let Some(val) = &self.val {
            write!(f, " {val}")?;
        }
        Ok(())
    }
}

impl Display for Logical {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Logical::And => write!(f, "&&"),
            Logical::Or => write!(f, "||"),
        }
    }
}

impl Display for Comparison {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Comparison::Eq => write!(f, "=="),
            Comparison::NotEq => write!(f, "!="),
            Comparison::Lt => write!(f, "<"),
            Comparison::LtEq => write!(f, "<="),
            Comparison::Gt => write!(f, ">"),
            Comparison::GtEq => write!(f, ">="),
            Comparison::In => write!(f, "IN"),
            Comparison::NotIn => write!(f, "NOT IN"),
            Comparison::Like => write!(f, "LIKE"),
            Comparison::NotLike => write!(f, "NOT LIKE"),
            Comparison::Match => write!(f, "=~"),
            Comparison::NotMatch => write!(f, "!~"),
            Comparison::AllIn => write!(f, "ALL IN"),
            Comparison::NoneIn => write!(f, "NONE IN"),
            Comparison::AnyIn => write!(f, "ANY IN"),
        }
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if self.sorts.is_empty() {
            return Ok(());
        }
        write!(f, "SORT ")?;
        let mut sort_iter = self.sorts.iter().peekable();
        while let Some(sort) = sort_iter.next() {
            write!(f, "{}", sort.var)?;
            if let SortDirection::Desc = sort.direction {
                write!(f, " DESC")?;
            }
            if sort_iter.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "LIMIT")?;
        if self.skip > 0 {
            write!(f, " {},", self.skip)?;
        }
        write!(f, " {}", self.limit)
    }
}

impl Display for Upsert {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "UPSERT {}\nINSERT {}\nUPDATE {}\nIN {}",
            self.search, self.insert.data, self.update.data, self.collection
        )?;
        if let Some(options) = &self.options {
            write!(f, " OPTIONS {options}")?;
        }
        Ok(())
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "INSERT {} INTO {}", self.data, self.collection)?;
        if let Some(options) = &self.options {
            write!(f, " OPTIONS {options}")?;
        }
        Ok(())
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "UPDATE")?;
        if let Some(var) = &self.var {
            write!(f, " {var} WITH")?;
        }
        write!(f, " {} IN {}", self.data, self.collection)?;
        if let Some(options) = &self.options {
            write!(f, " OPTIONS {options}")?;
        }
        Ok(())
    }
}

impl Display for Remove {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "REMOVE {} IN {}", self.key, self.collection)?;
        if let Some(options) = &self.options {
            write!(f, " OPTIONS {options}")?;
        }
        Ok(())
    }
}

impl Display for Return {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "RETURN")?;
        if self.distinct {
            write!(f, " DISTINCT")?;
        }
        match &self.merge {
            Some(merge) => {
                write!(f, " MERGE({}, ", self.var)?;
                if let Some(pre_merge) = &self.pre_merge {
                    write!(f, "{pre_merge} ")?;
                }
                write!(f, "{{ {} }}", merge)?;
                if let Some(post_merge) = &self.post_merge {
                    write!(f, " {post_merge}")?;
                }
                write!(f, ")")?;
            }
            None => write!(f, " {}", self.var)?,
        }
        Ok(())
    }
}

impl Display for PropMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        for (index, (prop, vars)) in self.0.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            match vars.len() {
                0 => write!(f, "{}", prop)?,
                1 => write!(f, "{}: {}", prop, vars.first().unwrap())?,
                _ => write!(f, "{}: FLATTEN([{}])", prop, vars.join(", "))?,
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use ontol_test_utils::expect_eq;
    use serde_json::json;
    use std::vec;

    #[test]
    fn test_document() {
        let aql = Query {
            selection: Some(Selection::Document("doc".to_string(), "@_id".to_string())),
            returns: Return {
                var: "doc".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            LET doc = DOCUMENT(@_id)
            RETURN doc
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_simple_forloop() {
        let aql = Query {
            selection: Some(Selection::Loop(For {
                var: "obj".to_string(),
                object: "@@collection".to_string(),
                ..Default::default()
            })),
            returns: Return {
                var: "obj".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            FOR obj IN @@collection
                RETURN obj
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_full_forloop() {
        let aql = Query {
            with: Some(vec!["foos".to_string()]),
            selection: Some(Selection::Loop(For {
                var: "obj".to_string(),
                object: "foos".to_string(),
                ..Default::default()
            })),
            operations: Some(vec![
                Operation::Filter(Filter {
                    var: "obj._class".to_string(),
                    comp: Some(Comparison::Like),
                    val: Some(r#""foos/%""#.to_string()),
                }),
                Operation::Filter(Filter {
                    var: "obj.thing".to_string(),
                    comp: Some(Comparison::Lt),
                    val: Some(r#""thong""#.to_string()),
                }),
                Operation::Sort(Sort {
                    sorts: vec![
                        SortExpression {
                            var: "obj.thing".to_string(),
                            direction: SortDirection::Asc,
                        },
                        SortExpression {
                            var: "obj.thang".to_string(),
                            direction: SortDirection::Desc,
                        },
                    ],
                }),
                Operation::Limit(Limit {
                    limit: 100,
                    ..Default::default()
                }),
            ]),
            returns: Return {
                var: "obj".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            WITH foos
            FOR obj IN foos
                FILTER obj._class LIKE "foos/%"
                FILTER obj.thing < "thong"
                SORT obj.thing, obj.thang DESC
                LIMIT 100
                RETURN obj
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_with_subquery() {
        let aql = Query {
            with: Some(vec![
                "foos".to_string(),
                "has_bars".to_string(),
                "bars".to_string(),
            ]),
            selection: Some(Selection::Loop(For {
                var: "obj".to_string(),
                object: "foos".to_string(),
                ..Default::default()
            })),
            operations: Some(vec![
                Operation::Filter(Filter {
                    var: "obj._class".to_string(),
                    comp: Some(Comparison::Like),
                    val: Some(r#""foos/%""#.to_string()),
                }),
                Operation::Let(Let {
                    var: "subobj_relation".to_string(),
                    query: Query {
                        selection: Some(Selection::Loop(For {
                            var: "subobj".to_string(),
                            edge: Some("subobj_edge".to_string()),
                            depth: Some(1..2),
                            direction: Some(Direction::Outbound),
                            object: "obj".to_string(),
                            edges: Some(vec!["has_bars".to_string()]),
                        })),
                        operations: Some(vec![Operation::Filter(Filter {
                            var: "subobj.index".to_string(),
                            comp: Some(Comparison::In),
                            val: Some("[1, 2, 3]".to_string()),
                        })]),
                        returns: Return {
                            var: "subobj".to_string(),
                            distinct: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
                Operation::Filter(Filter {
                    var: "LENGTH(subobj_relation)".to_string(),
                    ..Default::default()
                }),
                Operation::Limit(Limit {
                    skip: 10,
                    limit: 100,
                }),
            ]),
            returns: Return {
                var: "obj".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            WITH foos, has_bars, bars
            FOR obj IN foos
                FILTER obj._class LIKE "foos/%"
                LET subobj_relation = (
                    FOR subobj, subobj_edge IN 1..2 OUTBOUND obj has_bars
                        FILTER subobj.index IN [1, 2, 3]
                        RETURN DISTINCT subobj
                )
                FILTER LENGTH(subobj_relation)
                LIMIT 10, 100
                RETURN obj
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_insert() {
        let aql = Query {
            operations: Some(vec![Operation::Insert(Insert {
                data: json!({
                    "some_field": "some_value"
                })
                .to_string(),
                collection: "foos".to_string(),
                ..Default::default()
            })]),
            returns: Return {
                var: "obj".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            INSERT {"some_field":"some_value"} INTO foos
            RETURN obj
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_let_data() {
        let aql = Query {
            operations: Some(vec![
                Operation::LetData(LetData {
                    var: "test_data".to_string(),
                    data: vec![
                        r#"{"a": "test 1"}"#.to_string(),
                        r#"{"a": "test 2"}"#.to_string(),
                        r#"{"a": "test 3"}"#.to_string(),
                    ],
                }),
                Operation::Let(Let {
                    var: "test".to_string(),
                    query: Query {
                        selection: Some(Selection::Loop(For {
                            var: "item".to_string(),
                            object: "test_data".to_string(),
                            ..Default::default()
                        })),
                        operations: Some(vec![Operation::Insert(Insert {
                            data: "item".to_string(),
                            collection: "collection".to_string(),
                            options: Some(r#"{overwriteMode: "ignore"}"#.to_string()),
                        })]),
                        returns: Return {
                            var: "NEW".to_string(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
            ]),
            returns: Return {
                var: "test".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            LET test_data = [
                {"a": "test 1"},
                {"a": "test 2"},
                {"a": "test 3"}
            ]
            LET test = (
                FOR item IN test_data
                    INSERT item INTO collection OPTIONS {overwriteMode: "ignore"}
                    RETURN NEW
            )
            RETURN test
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_insert_relation() {
        let aql = Query {
            with: Some(vec!["members".to_string(), "Organization".to_string()]),
            operations: Some(vec![
                Operation::Selection(Selection::Document(
                    "Organization".to_string(),
                    r#""Organization/lolsoft""#.to_string(),
                )),
                Operation::Let(Let {
                    var: "Organization_members_User_rel".to_string(),
                    query: Query {
                        operations: Some(vec![Operation::Insert(Insert {
                            data: r#"{ _from: Organization._id, _to: "User/bob", "role":"contributor" }"#.to_string(),
                            collection: "members".to_string(),
                            ..Default::default()
                        })]),
                        returns: Return {
                            var: "NEW".to_string(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
            ]),
            returns: Return {
                var: "Organization".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            WITH members, Organization
            LET Organization = DOCUMENT("Organization/lolsoft")
            LET Organization_members_User_rel = (
                INSERT { _from: Organization._id, _to: "User/bob", "role":"contributor" } INTO members
                RETURN NEW
            )
            RETURN Organization
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_update_relation() {
        let aql = Query {
            with: Some(vec![
                "members".to_string(),
                "Organization".to_string(),
                "User".to_string(),
            ]),
            operations: Some(vec![
                Operation::Selection(Selection::Document(
                    "Organization".to_string(),
                    r#""Organization/lolsoft""#.to_string(),
                )),
                Operation::Let(Let {
                    var: "Organization_members_User".to_string(),
                    query: Query {
                        selection: Some(Selection::Loop(For {
                            var: "obj".to_string(),
                            edge: Some("obj_edge".to_string()),
                            direction: Some(Direction::Outbound),
                            object: "Organization".to_string(),
                            edges: Some(vec!["members".to_string()]),
                            ..Default::default()
                        })),
                        operations: Some(vec![
                            Operation::Filter(Filter {
                                var: "obj._key".to_string(),
                                comp: Some(Comparison::Eq),
                                val: Some(r#""bob""#.to_string()),
                            }),
                            Operation::Let(Let {
                                var: "_edge".to_string(),
                                query: Query {
                                    operations: Some(vec![Operation::Update(Update {
                                        var: Some("obj_edge".to_string()),
                                        data: r#"{ "role":"admin" }"#.to_string(),
                                        collection: "members".to_string(),
                                        ..Default::default()
                                    })]),
                                    returns: Return {
                                        var: "NEW".to_string(),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                },
                            }),
                        ]),
                        returns: Return {
                            var: "obj".to_string(),
                            merge: Some("_edge".to_string()),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
            ]),
            returns: Return {
                var: "Organization".to_string(),
                merge: Some("members: Organization_members_User".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            WITH members, Organization, User
            LET Organization = DOCUMENT("Organization/lolsoft")
            LET Organization_members_User = (
                FOR obj, obj_edge IN OUTBOUND Organization members
                    FILTER obj._key == "bob"
                    LET _edge = (
                        UPDATE obj_edge WITH { "role":"admin" } IN members
                        RETURN NEW
                    )
                    RETURN MERGE(obj, { _edge })
            )
            RETURN MERGE(Organization, { members: Organization_members_User })
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_delete_relation() {
        let aql = Query {
            with: Some(vec![
                "members".to_string(),
                "Organization".to_string(),
                "User".to_string(),
            ]),
            operations: Some(vec![
                Operation::Selection(Selection::Document(
                    "Organization".to_string(),
                    r#""Organization/lolsoft""#.to_string(),
                )),
                Operation::Let(Let {
                    var: "Organization_members_User".to_string(),
                    query: Query {
                        selection: Some(Selection::Loop(For {
                            var: "obj".to_string(),
                            edge: Some("obj_edge".to_string()),
                            direction: Some(Direction::Outbound),
                            object: "Organization".to_string(),
                            edges: Some(vec!["members".to_string()]),
                            ..Default::default()
                        })),
                        operations: Some(vec![
                            Operation::Filter(Filter {
                                var: "obj._key".to_string(),
                                comp: Some(Comparison::Eq),
                                val: Some(r#""bob""#.to_string()),
                            }),
                            Operation::Let(Let {
                                var: "_edge".to_string(),
                                query: Query {
                                    operations: Some(vec![Operation::Remove(Remove {
                                        key: r#"obj_edge"#.to_string(),
                                        collection: "members".to_string(),
                                        ..Default::default()
                                    })]),
                                    returns: Return {
                                        var: "OLD".to_string(),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                },
                            }),
                        ]),
                        returns: Return {
                            var: "obj".to_string(),
                            merge: Some("_edge".to_string()),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
            ]),
            returns: Return {
                var: "Organization".to_string(),
                merge: Some("members: Organization_members_User".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            WITH members, Organization, User
            LET Organization = DOCUMENT("Organization/lolsoft")
            LET Organization_members_User = (
                FOR obj, obj_edge IN OUTBOUND Organization members
                    FILTER obj._key == "bob"
                    LET _edge = (
                        REMOVE obj_edge IN members
                        RETURN OLD
                    )
                    RETURN MERGE(obj, { _edge })
            )
            RETURN MERGE(Organization, { members: Organization_members_User })
            "#},
            actual = format!("{aql}")
        );
    }

    #[test]
    fn test_overwrite_relation() {
        let aql = Query {
            with: Some(vec![
                "owner".to_string(),
                "Organization".to_string(),
                "User".to_string(),
                "Repository".to_string(),
            ]),
            operations: Some(vec![
                Operation::Selection(Selection::Document(
                    "User".to_string(),
                    r#""User/bob""#.to_string(),
                )),
                Operation::Selection(Selection::Document(
                    "Repository".to_string(),
                    r#""Repository/018df9b2-a4c1-7947-913b-3023e318517e""#.to_string(),
                )),
                Operation::Let(Let {
                    var: "Repository_owner".to_string(),
                    query: Query {
                        selection: Some(Selection::Loop(For {
                            var: "obj".to_string(),
                            edge: Some("obj_edge".to_string()),
                            direction: Some(Direction::Outbound),
                            object: "Repository".to_string(),
                            edges: Some(vec!["owner".to_string()]),
                            ..Default::default()
                        })),
                        operations: Some(vec![Operation::Let(Let {
                            var: "_edge".to_string(),
                            query: Query {
                                operations: Some(vec![Operation::Update(Update {
                                    var: Some("obj_edge".to_string()),
                                    data: r#"{ _to: User._id }"#.to_string(),
                                    collection: "owner".to_string(),
                                    ..Default::default()
                                })]),
                                returns: Return {
                                    var: "NEW".to_string(),
                                    ..Default::default()
                                },
                                ..Default::default()
                            },
                        })]),
                        returns: Return {
                            var: "obj".to_string(),
                            merge: Some("_edge".to_string()),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
            ]),
            returns: Return {
                var: "User".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        expect_eq!(
            expected = indoc! {r#"
            WITH owner, Organization, User, Repository
            LET User = DOCUMENT("User/bob")
            LET Repository = DOCUMENT("Repository/018df9b2-a4c1-7947-913b-3023e318517e")
            LET Repository_owner = (
                FOR obj, obj_edge IN OUTBOUND Repository owner
                    LET _edge = (
                        UPDATE obj_edge WITH { _to: User._id } IN owner
                        RETURN NEW
                    )
                    RETURN MERGE(obj, { _edge })
            )
            RETURN User
            "#},
            actual = format!("{aql}")
        );
    }
}
