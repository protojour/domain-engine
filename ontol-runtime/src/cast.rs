use smartstring::alias::String;
use uuid::Uuid;

use crate::value::{Attribute, Value};

/// Cast, or panic at runtime
pub trait Cast<T> {
    type Ref: ?Sized;

    fn cast_into(self) -> T;

    fn cast_ref(&self) -> &Self::Ref;
}

impl Cast<Value> for Value {
    type Ref = Value;

    fn cast_into(self) -> Value {
        self
    }

    fn cast_ref(&self) -> &Self::Ref {
        self
    }
}

impl Cast<()> for Value {
    type Ref = ();

    fn cast_into(self) {
        match self {
            Value::Unit(_) => (),
            _ => panic!("not a unit"),
        }
    }

    fn cast_ref(&self) -> &Self::Ref {
        match self {
            Value::Unit(_) => &(),
            _ => panic!("not a unit"),
        }
    }
}

impl Cast<bool> for Value {
    type Ref = bool;

    fn cast_into(self) -> bool {
        match self {
            Value::I64(i, _) => i != 0,
            _ => panic!("not a unit"),
        }
    }

    fn cast_ref(&self) -> &Self::Ref {
        match self {
            Value::I64(i, _) => {
                if *i != 0 {
                    &true
                } else {
                    &false
                }
            }
            _ => panic!("not a unit"),
        }
    }
}

impl Cast<String> for Value {
    type Ref = str;

    fn cast_into(self) -> String {
        match self {
            Value::Text(s, _) => s,
            _ => panic!("not a string"),
        }
    }

    fn cast_ref(&self) -> &Self::Ref {
        match self {
            Value::Text(s, _) => s.as_str(),
            _ => panic!("not a string"),
        }
    }
}

impl Cast<i64> for Value {
    type Ref = i64;

    fn cast_into(self) -> i64 {
        match self {
            Value::I64(n, _) => n,
            _ => panic!("not an integer"),
        }
    }

    fn cast_ref(&self) -> &Self::Ref {
        match self {
            Value::I64(n, _) => n,
            _ => panic!("not an integer"),
        }
    }
}

impl Cast<Vec<Attribute>> for Value {
    type Ref = [Attribute];

    fn cast_into(self) -> Vec<Attribute> {
        match self {
            Value::Sequence(seq, _) => seq.attrs.into_iter().collect(),
            _ => panic!("not an vector"),
        }
    }

    fn cast_ref(&self) -> &Self::Ref {
        match self {
            Value::Sequence(seq, _) => seq.attrs.as_slice(),
            _ => panic!("not a vector"),
        }
    }
}

impl Cast<Uuid> for Value {
    type Ref = Uuid;

    fn cast_into(self) -> Uuid {
        match self {
            Value::OctetSequence(seq, _) => Uuid::from_slice(&seq).unwrap(),
            _ => panic!("not an uuid"),
        }
    }

    fn cast_ref(&self) -> &Self::Ref {
        panic!("Uuid cannot be cast as ref")
    }
}
