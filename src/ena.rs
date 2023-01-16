use std::collections::HashMap;

use ena::unify;

struct Namespace {
    types: HashMap<String, Type>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum Type {
    Number,
    New(&'static str, Box<Type>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum Expr {
    Constant(i32),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Constr(&'static str, Box<Expr>),
    Variable(u32, &'static str),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
struct Key(u32);

impl ena::unify::UnifyKey for Key {
    type Value = Value;

    fn index(&self) -> u32 {
        self.0
    }

    fn from_index(u: u32) -> Self {
        Self(u)
    }

    fn tag() -> &'static str {
        "ontol"
    }
}

#[derive(Clone, Debug)]
enum Value {
    Unknown,
    Known(Expr),
}

impl ena::unify::UnifyValue for Value {
    type Error = ena::unify::NoError;

    fn unify_values(value1: &Self, value2: &Self) -> Result<Self, Self::Error> {
        match (value1, value2) {
            (Self::Known(_), Self::Known(_)) => {
                panic!("tried to unify two knowns: {value1:?} and {value2:?}")
            }
            (Self::Known(v), Self::Unknown) => Ok(Self::Known(v.clone())),
            (Self::Unknown, Self::Known(v)) => Ok(Self::Known(v.clone())),
            (Self::Unknown, Self::Unknown) => Ok(Self::Unknown),
        }
    }
}

struct Inference {
    eq_relations: ena::unify::InPlaceUnificationTable<Key>,
}

impl Inference {
    fn new() -> Self {
        Self {
            eq_relations: Default::default(),
        }
    }

    fn new_variable(&mut self, name: &'static str) -> Expr {
        Expr::Variable(self.eq_relations.new_key(Value::Unknown).0, name)
    }

    fn huh(&mut self) {
        self.eq_relations.union(Key(1), Key(2));
    }
}

fn test_namespace() -> Namespace {
    Namespace {
        types: [
            ("m".into(), Type::New("m", Box::new(Type::Number))),
            ("mm".into(), Type::New("mm", Box::new(Type::Number))),
        ]
        .into(),
    }
}

#[test]
fn uh_great() {
    let mut inf = Inference::new();

    let var = inf.new_variable("x");

    let key0 = inf.eq_relations.new_key(Value::Unknown);

    inf.eq_relations
        .union_value(key0, Value::Known(Expr::Constant(42)));

    let eh = inf.eq_relations.inlined_probe_value(key0);

    let Value::Known(ty) = eh else {
        panic!("Not known");
    };

    assert_eq!(Expr::Constant(42), ty);

    // inf.huh();
}

#[test]
fn equate_expr() {
    let mut inf = Inference::new();

    let left_root = inf.eq_relations.new_key(Value::Unknown);
    let right_root = inf.eq_relations.new_key(Value::Unknown);
    let x = inf.eq_relations.new_key(Value::Unknown);

    inf.eq_relations.union_value(
        left_root,
        Value::Known(Expr::Constr("meters", Box::new(Expr::Variable(x.0, "x")))),
    );
    inf.eq_relations.union_value(
        right_root,
        Value::Known(Expr::Constr(
            "millimeters",
            Box::new(Expr::Mul(
                Box::new(Expr::Variable(x.0, "x")),
                Box::new(Expr::Constant(1000)),
            )),
        )),
    );

    //    inf.eq_relations.unify_var_var(a_id, b_id)

    inf.eq_relations.union(left_root, right_root);

    // inf.huh();
}
