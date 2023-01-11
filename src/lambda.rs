struct Reg<'e, T> {
    value: Option<Value<T>>,
    lambda: Lambda<'e, T>,
}

enum Value<T> {
    Constant(T),
    Variable(T),
}

enum Lambda<'e, T> {
    Native(&'e dyn NativeFn<T>),
}

trait NativeFn<T> {
    fn exec(&self, reg: &mut Reg<'_, T>) -> T;
}

enum Builtin {
    Add,
    Sub,
    Mul,
    Div,
}

struct Plus {
    value: Value<i32>,
}

impl NativeFn<i32> for Plus {
    fn exec(&self, reg: &mut Reg<'_, i32>) -> i32 {
        0
    }
}
