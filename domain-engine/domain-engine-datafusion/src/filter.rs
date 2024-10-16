use std::collections::BTreeMap;

use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use domain_engine_arrow::schema::{iter_arrow_fields, FieldType};
use domain_engine_core::domain_select;
use ontol_runtime::{
    ontology::{aspects::DefsAspect, domain::Def},
    query::{
        condition::{Clause, CondTerm, Condition, SetOperator, SetPredicate},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    value::Value,
    var::Var,
    DefId, PropId,
};
use tracing::trace;

#[derive(Clone)]
pub struct DatafusionFilter {
    entity_select: EntitySelect,
    column_selection: Vec<(PropId, FieldType)>,
    projection: Option<Vec<usize>>,
}

impl DatafusionFilter {
    pub fn compile(
        def_id: DefId,
        (projection, filters, limit): (Option<&Vec<usize>>, &[Expr], Option<usize>),
        ontology_defs: &DefsAspect,
    ) -> Self {
        let def = ontology_defs.def(def_id);
        let mut select_properties: BTreeMap<PropId, Select> = Default::default();
        let mut columns = vec![];

        let fields: Vec<_> = iter_arrow_fields(def, ontology_defs, vec![]).collect();

        if let Some(projection) = projection {
            for field_idx in projection {
                let Some(field_info) = fields.get(*field_idx) else {
                    continue;
                };

                select_properties.insert(
                    field_info.prop_id,
                    domain_select::domain_select_no_edges(field_info.field_def_id, ontology_defs),
                );
                columns.push((field_info.prop_id, field_info.field_type));
            }
        } else {
            if let Select::Struct(struct_select) =
                domain_select::domain_select_no_edges(def_id, ontology_defs)
            {
                select_properties = struct_select.properties;
            }

            columns = fields
                .into_iter()
                .map(|field_info| (field_info.prop_id, field_info.field_type))
                .collect();
        }

        let mut ontol_filter = Filter::default_for_domain();

        {
            let mut condition_builder = ConditionBuilder::new(def, ontology_defs);
            for expr in filters {
                let _ = condition_builder.try_add_expr(expr);
            }

            (*ontol_filter.condition_mut()) = condition_builder.build();
        }

        DatafusionFilter {
            entity_select: EntitySelect {
                source: StructOrUnionSelect::Struct(StructSelect {
                    def_id,
                    properties: select_properties,
                }),
                filter: ontol_filter,
                limit,
                after_cursor: None,
                include_total_len: false,
            },
            column_selection: columns,
            projection: projection.cloned(),
        }
    }

    pub fn entity_select(&self) -> EntitySelect {
        self.entity_select.clone()
    }

    pub fn column_selection(&self) -> Vec<(PropId, FieldType)> {
        self.column_selection.clone()
    }

    pub fn projection(&self) -> Option<Vec<usize>> {
        self.projection.clone()
    }
}

pub struct ConditionBuilder<'o> {
    condition: Condition,
    root_var: Var,
    has_filters: bool,
    def: &'o Def,
    ontology_defs: &'o DefsAspect,
}

pub enum ConditionError {
    Expr,
    BinaryOperands,
    BinaryOperator,
    Column,
    Literal,
    TimeZone,
}

impl<'o> ConditionBuilder<'o> {
    pub fn new(def: &'o Def, ontology_defs: &'o DefsAspect) -> Self {
        let mut condition = Condition::default();
        let root_var = condition.mk_cond_var();
        condition.add_clause(root_var, Clause::Root);

        Self {
            condition,
            root_var,
            has_filters: false,
            def,
            ontology_defs,
        }
    }

    pub fn build(self) -> Condition {
        if self.has_filters {
            self.condition
        } else {
            Condition::default()
        }
    }

    pub fn try_add_expr(&mut self, expr: &Expr) -> Result<(), ConditionError> {
        trace!("try add expr: {expr:?}");

        let result = match expr {
            Expr::BinaryExpr(binary) => self.try_add_binary(binary),
            _ => Err(ConditionError::Expr),
        };

        if result.is_ok() {
            self.has_filters = true;
        }

        result
    }

    fn try_add_binary(&mut self, expr: &BinaryExpr) -> Result<(), ConditionError> {
        let mut predicate_op = PredicateOp::try_from(expr.op)?;
        let mut operands = [
            self.classify_as_scalar_expr(&expr.left)?,
            self.classify_as_scalar_expr(&expr.right)?,
        ];

        // want the property first, then the literal:
        if operands[0].ordinal() > operands[1].ordinal() {
            operands.swap(0, 1);
            predicate_op = predicate_op.invert();
        }

        if let [ScalarExpr::Property(prop_id), ScalarExpr::Literal(lit)] = operands {
            match predicate_op {
                PredicateOp::Eq => {
                    let set_var = self.condition.mk_cond_var();
                    self.condition.add_clause(
                        self.root_var,
                        Clause::MatchProp(prop_id, SetOperator::ElementIn, set_var),
                    );
                    self.condition.add_clause(
                        set_var,
                        Clause::Member(CondTerm::Wildcard, CondTerm::Value(lit)),
                    );
                    Ok(())
                }
                PredicateOp::Lt => self.add_set_predicate(prop_id, lit, SetPredicate::Lt),
                PredicateOp::LtEq => self.add_set_predicate(prop_id, lit, SetPredicate::Lte),
                PredicateOp::Gt => self.add_set_predicate(prop_id, lit, SetPredicate::Gt),
                PredicateOp::GtEq => self.add_set_predicate(prop_id, lit, SetPredicate::Gte),
            }
        } else {
            Err(ConditionError::BinaryOperands)
        }
    }

    fn add_set_predicate(
        &mut self,
        prop_id: PropId,
        lit: Value,
        pred: SetPredicate,
    ) -> Result<(), ConditionError> {
        let set_var = self.condition.mk_cond_var();
        self.condition.add_clause(
            self.root_var,
            Clause::MatchProp(prop_id, SetOperator::ElementIn, set_var),
        );
        self.condition
            .add_clause(set_var, Clause::SetPredicate(pred, CondTerm::Value(lit)));
        Ok(())
    }

    fn classify_as_scalar_expr(&self, expr: &Expr) -> Result<ScalarExpr, ConditionError> {
        fn lit<T>(lit: &Option<T>) -> Result<&T, ConditionError> {
            lit.as_ref().ok_or(ConditionError::Literal)
        }

        fn no_tz<T>(tz: &Option<T>) -> Result<(), ConditionError> {
            if tz.is_some() {
                Err(ConditionError::TimeZone)
            } else {
                Ok(())
            }
        }

        match expr {
            Expr::Column(column) => {
                if let Some((prop_id, _)) = self
                    .def
                    .data_relationships
                    .iter()
                    .find(|(_, rel_info)| self.ontology_defs[rel_info.name] == column.name)
                {
                    Ok(ScalarExpr::Property(*prop_id))
                } else {
                    Err(ConditionError::Column)
                }
            }
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(b) => Ok(ScalarExpr::Literal(Value::boolean(*lit(b)?))),
                ScalarValue::Float16(f) => Ok(ScalarExpr::Literal(Value::f64((*lit(f)?).into()))),
                ScalarValue::Float32(f) => Ok(ScalarExpr::Literal(Value::f64((*lit(f)?).into()))),
                ScalarValue::Float64(f) => Ok(ScalarExpr::Literal(Value::f64(*lit(f)?))),
                ScalarValue::Int8(i) => Ok(ScalarExpr::Literal(Value::i64((*lit(i)?).into()))),
                ScalarValue::Int16(i) => Ok(ScalarExpr::Literal(Value::i64((*lit(i)?).into()))),
                ScalarValue::Int32(i) => Ok(ScalarExpr::Literal(Value::i64((*lit(i)?).into()))),
                ScalarValue::Int64(i) => Ok(ScalarExpr::Literal(Value::i64(*lit(i)?))),
                ScalarValue::UInt8(i) => Ok(ScalarExpr::Literal(Value::i64((*lit(i)?).into()))),
                ScalarValue::UInt16(i) => Ok(ScalarExpr::Literal(Value::i64((*lit(i)?).into()))),
                ScalarValue::UInt32(i) => Ok(ScalarExpr::Literal(Value::i64((*lit(i)?).into()))),
                ScalarValue::UInt64(i) => Ok(ScalarExpr::Literal(Value::i64(
                    (*lit(i)?).try_into().map_err(|_| ConditionError::Literal)?,
                ))),
                ScalarValue::Utf8(s) => Ok(ScalarExpr::Literal(Value::text(lit(s)?))),
                ScalarValue::TimestampSecond(s, tz) => {
                    no_tz(tz)?;
                    Ok(ScalarExpr::Literal(Value::datetime(
                        chrono::DateTime::from_timestamp(*lit(s)?, 0)
                            .ok_or(ConditionError::Literal)?,
                    )))
                }
                ScalarValue::TimestampMillisecond(ms, tz) => {
                    no_tz(tz)?;
                    Ok(ScalarExpr::Literal(Value::datetime(
                        chrono::DateTime::from_timestamp_millis(*lit(ms)?)
                            .ok_or(ConditionError::Literal)?,
                    )))
                }
                ScalarValue::TimestampMicrosecond(ms, tz) => {
                    no_tz(tz)?;
                    Ok(ScalarExpr::Literal(Value::datetime(
                        chrono::DateTime::from_timestamp_micros(*lit(ms)?)
                            .ok_or(ConditionError::Literal)?,
                    )))
                }
                ScalarValue::TimestampNanosecond(ns, tz) => {
                    no_tz(tz)?;
                    Ok(ScalarExpr::Literal(Value::datetime(
                        chrono::DateTime::from_timestamp_nanos(*lit(ns)?),
                    )))
                }
                ScalarValue::Binary(b) => {
                    let bytes = lit(b)?;
                    Ok(ScalarExpr::Literal(Value::octet_sequence(
                        bytes.iter().copied().collect(),
                    )))
                }
                ScalarValue::Null => Err(ConditionError::Literal),
                ScalarValue::Decimal128(_, _, _) => Err(ConditionError::Literal),
                ScalarValue::Decimal256(_, _, _) => Err(ConditionError::Literal),
                ScalarValue::Utf8View(_) => Err(ConditionError::Literal),
                ScalarValue::LargeUtf8(_) => Err(ConditionError::Literal),
                ScalarValue::BinaryView(_) => Err(ConditionError::Literal),
                ScalarValue::FixedSizeBinary(_, _) => Err(ConditionError::Literal),
                ScalarValue::LargeBinary(_) => Err(ConditionError::Literal),
                ScalarValue::FixedSizeList(_) => Err(ConditionError::Literal),
                ScalarValue::List(_) => Err(ConditionError::Literal),
                ScalarValue::LargeList(_) => Err(ConditionError::Literal),
                ScalarValue::Struct(_) => Err(ConditionError::Literal),
                ScalarValue::Map(_) => Err(ConditionError::Literal),
                ScalarValue::Date32(_) => Err(ConditionError::Literal),
                ScalarValue::Date64(_) => Err(ConditionError::Literal),
                ScalarValue::Time32Second(_) => Err(ConditionError::Literal),
                ScalarValue::Time32Millisecond(_) => Err(ConditionError::Literal),
                ScalarValue::Time64Microsecond(_) => Err(ConditionError::Literal),
                ScalarValue::Time64Nanosecond(_) => Err(ConditionError::Literal),
                ScalarValue::IntervalYearMonth(_) => Err(ConditionError::Literal),
                ScalarValue::IntervalDayTime(_) => Err(ConditionError::Literal),
                ScalarValue::IntervalMonthDayNano(_) => Err(ConditionError::Literal),
                ScalarValue::DurationSecond(_) => Err(ConditionError::Literal),
                ScalarValue::DurationMillisecond(_) => Err(ConditionError::Literal),
                ScalarValue::DurationMicrosecond(_) => Err(ConditionError::Literal),
                ScalarValue::DurationNanosecond(_) => Err(ConditionError::Literal),
                ScalarValue::Union(_, _, _) => Err(ConditionError::Literal),
                ScalarValue::Dictionary(_, _) => Err(ConditionError::Literal),
            },
            _ => Err(ConditionError::Expr),
        }
    }
}

enum ScalarExpr {
    Property(PropId),
    Literal(Value),
}

impl ScalarExpr {
    fn ordinal(&self) -> u8 {
        match self {
            Self::Property(_) => 0,
            Self::Literal(_) => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum PredicateOp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl TryFrom<Operator> for PredicateOp {
    type Error = ConditionError;

    fn try_from(value: Operator) -> Result<Self, Self::Error> {
        match value {
            Operator::Eq => Ok(Self::Eq),
            Operator::Lt => Ok(Self::Lt),
            Operator::LtEq => Ok(Self::LtEq),
            Operator::Gt => Ok(Self::Gt),
            Operator::GtEq => Ok(Self::GtEq),
            _ => Err(ConditionError::BinaryOperator),
        }
    }
}

impl PredicateOp {
    fn invert(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Lt => Self::GtEq,
            Self::LtEq => Self::Gt,
            Self::Gt => Self::LtEq,
            Self::GtEq => Self::Lt,
        }
    }
}
