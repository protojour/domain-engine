# `map`

A `map` statement describes how one [`def`](def.md) maps to another (and sometimes vice versa) — how data flows between them.

They are often used for expressing transformations, or in _domain translation_, i.e. how a `def` imported from a foreign domain maps to a `def` in the current domain, and ultimately describe how data can be flow from [data stores](data_stores.md) to [interfaces](interfaces.md) and back again.

The `map` statement has two arms, each containing a `def` identifer. For mapping values between one arm and another, we use variables. Variable names can be anything, you can make them up as you go along.

The simplest map statements define an _equivalence_ between two `def`s; this is a _bijective_ function, or simply a _two-way_ map.

In a two-way mapping, all leaf properties (properties with a value) must be accounted for, all variables mentioned must be present in both arms.

```ontol
def Person (
    rel .'Name': text
    rel .'Born': datetime
    rel .'Died'?: datetime
)

def citizen (
    rel .'name': text
    rel .'life_data': life
)

def life (
    rel .'dob': datetime
    rel .'rip'?: datetime
)

map (
    Person(                 // first map arm
        'Name': n,          // the 'Name' property and a variable, n
        'Born': b,          // ...
        'Died'?: d,         // optional marker as in def
    ),
    citizen(                // second map arm
        'name': n,          // 'name' gets value of n, above
        'life_data': life(  // sub-arm representing nested def
            'dob': b,       // 'dob' gets value of b, above
            'rip'?: d,      // ...
        )
    )
)
```

As you can see we've described how a `Person` is renamed and restructured into a `citizen`, with a nested `life` record. But the opposite is also possible: A `citizen` can be mapped to a `Person`, with no loss of data.


## `@match map`

If one map arm is preceded by the `@match` modifier, the semantics change to a _one-way_ map, its arms allow _non-equivalence_. Creating a one-way map doesn't mean an equivalence _can't_ exist, it just describes a partial mapping.

Let's say `Person` only had the properties `'Name'` and `'Born'`, and that a value for `'rip'` is required (maybe they're Roman citizens):

```ontol
def Person (
    rel .'Name': text
    rel .'Born': datetime
)

def citizen (
    rel .'name': text
    rel .'life_data': life
)

def life (
    rel .'dob': datetime
    rel .'rip': datetime
)

map (
    Person(
        'Name': n,
        'Born': b,
    ),
    @match citizen(
        'name': n,
        'life_data': life(
            'dob': b,
        )
    )
)
```

When using `@match`, `Person` (the non-`@match` arm) must have all properties accounted for, but `citizen` (the `@match` arm) does not (all variables must still match).

In this example, `Person` can be extracted from a `citizen`, but not vice versa; the value of `'rip'` gets dropped along the way, and there's no way of getting it back from a `Person`, so there is data loss. An equivalence _can't_ exist between `Person` and `citizen`.

However, if we were to make `'rip'` optional again (using `?`), a two-way mapping is possible, although we still allow data to be lost by discarding `'rip'` values.

```ontol
// ...

def life (
    rel .'dob': datetime
    rel .'rip'?: datetime
)

map (
    Person(
        'Name': n,
        'Born': b,
    ),
    citizen(
        'name': n,
        'life_data': life(
            'dob': b,
        )
    )
)
```


## `map` transitivity

If there exists a map `a <-> b` and a map `b <-> c`, it follows that it is possible to map `a <-> c`. In other words, `map`s are transitive, and the domain engine makes use of this transitivity when mapping data.

```ontol
def a ( rel .'a': text )
def b ( rel .'b': text )
def c ( rel .'c': text )

        'Died'?: d,
    ),
    citizen(
        'name': n,
        'life_data': life(
            'dob': b,
            'rip'?: d,
        )
    )
)
"#
        .to_string(),
    )
    .unwrap();

    let mut map = ont.types[3].clone().into_map().unwrap();
    assert_eq!(2, map.arms.len());

    // First arm is a Person
    match &mut map.arms[0] {
        MapArm::Map(arm) => {
            assert_eq!("Person", arm.name);

            let mut expected = vec![
                ("Name".to_string(), "n".to_string()),
                ("Born".to_string(), "b".to_string()),
                ("Died".to_string(), "d".to_string()),
            ];

            for (i, prop) in arm.properties.iter().enumerate() {
                let (name, var) = expected
map (
    a( 'a': x ),
    b( 'b': x ),
)

map (
    b( 'b': x ),
    c( 'c': x ),
)
```

Likewise if there exists a map `a <- @match b` and `b <- @match c`, there should be a transitive map `a <- @match c`.


##

## Named maps

Named `map` statements are used to express _queries_, and are further explored in the chapter on [_interfaces_}(interfaces.md).
