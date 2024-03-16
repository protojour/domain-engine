# `rel`

The `rel` statement defines a ***relationship*** in the form of a _triplet_; the first parameter is the _subject_, the second is the _relation_, and the third is the _object_.

In ONTOL, just about everything is connected to something via relationships.

The properties of a struct [`def`](def.md) are defined using relationships:

```ontol
rel subject 'relation_name': object
```

Here `subject` is the subject, `'relation_name'` names the relation itself, and `object` is the object. In other words some `def subject` has a property called `'relation_name'` which is some type called `object`.

A relation may be _optional_, denoted by a `?` in the relation:

```ontol
rel subject 'relation_name'?: object
```


## `.` or "self"

A common pattern is to define relationships within the block of a [`def`](def.md). In the scope of any block, the subject or object can be replaced by a `.`, meaning "self", and referring to the enclosing type. It is usually used for the subject

```ontol
def some_def (
    rel . 'relation_name': text
)
```

This is the same as:

```ontol
def some_def ()
rel some_def 'relation_name': text
```

ONTOL doesn't care much about spacing, and the `.` is often contracted into the relation:

```ontol
def some_def (
    rel .'relation_name': text
)
```


## `is` relationships

Not all relationships are simple named properties.

[`is`](relation_types.md#is) is a _relation type_ from the [`ontol` domain](ontol_domain.md) documented later, but it's such an invaluable basic building block.

`is`, used as a relation, gives the subject type all characteristics of the object type:

```ontol
def textlike (
    rel .is: text
)
```

`textlike` is now a type interchangable with — but separate from — `text`. It behaves the same for now, but may take on other, compatible characteristics.

`is` relationships have many uses, from field and structure re-use and composition, to [unions](#unions) (below):


```ontol
def named (
    rel .'name': text
)

def cat (
    rel .is: named
)

def copycat (
    rel .is: cat
    rel .'name': 'ccat'
)
```



## Unions

Unions can be defined using conditional `is?` relationships.

Union variants must have a discriminant so ONTOL can tell them apart; either their primary key structure is different or some inherent property is, such as a constant:

```ontol
def type_a (
    rel .'type': 'a'
)

def type_b (
    rel .'type': 'b'
)

def union_ab (
    rel .is?: type_a
    rel .is?: type_b
)
```

The union `union_ab` may now be used as any other type.

```ontol
def some_def (
    rel .'a_or_b': union_ab
)
```


## Sets

_Sets_ are expressed using `{}` parentheses.

```ontol
def some_def (
    rel .'texts': {text}
)
```

Here, the property `'texts'` is a set of `text`s.

Many things in ONTOL are represented as sets, including entity collections.

A value might be expressed e.g. `{'a', 'b', 'c'}`.


## Lists

_Lists_ are rarely used in ONTOL, but may be constructed using `[]` brackets.

```ontol
def some_def (
    rel .'texts': [text]
)
```

A value might be expressed e.g. `['a', 'b', 'c']` and adressed by index, `texts[0]`.


## Tuples

_Tuples_ can be expressed using `rel` with indexes.

```ontol
def text_tuple (
    rel .0: text,
    rel .1: text,
)
```

Here, the property `text_tuple` is a pair of `text` values; no more, no less.

Tuples can be of any (fixed) length, and members can be of any type. If its members are homogenous, this can be simplified using a _range_ as:

```ontol
def text_tuple (
    rel .0..2: text,
)
```

A value might be expressed e.g. `['a', 'b']` and adressed by index, `text_tuple[0]`.



## `rel` relationships and edge properties

A relation may have relationships of its own, indicated by a body delimited by square brackets (`[]`) within the relation parameter (usually before the `:`).

If there's only one `rel` statement, this is usually added inline, such as for [`default`](relation_types.md#default) and other relation types:

```ontol
def some_def (
    rel . 'active'[rel .default := true]: boolean
)
```

A relation may also "break out" its definition using an `is` relation:

```ontol
def person (
    rel .'id'|id: (rel .is: serial)
    rel .'best_friend'[rel .is: friendship]::'biggest_fan'? person
)

def friendship (
    rel .'likeness': f64
    rel .'openness': f64
    rel .'health': f64
    rel .'start': datetime
    rel .'end'?: datetime
)
```


## One-to-one entity relationship

`rel` statements always describe relationships between `def`s, the subject and object are types, either primitives

If the object is an [entity](entities.md) (i.e. an identifiable type), the relationship between them are represented by edges in a graph, and the relation can have different names on each end of the relationship. This is indicated by a `::` double colon operator:

```ontol
rel def_a 'DefB'::'DefA' def_b
```

Such a relationship can also be expressed as part of a `def` structure:

```ontol
def def_a (
    // ...
    rel . 'DefB'::'DefA' def_b
)
```

In a directed graph, the direction of the relationship is indicated by the order. The relationship goes from the subject to the object, or `def_a -> def_b`.


## One-to-many entity relationship

Cardinality is indicated by expressing the subject and/or object as a [set](#sets).

```ontol
rel def_a 'DefBs'::'DefA' {def_b}
```

Here, `def_a` has a property called `'DefBs'`, which is a relationship to one or more `def_b`s. Each `def_b` has a property called `'DefA'`, which is a relationship to one `def_a`.


## Many-to-many entity relationship

```ontol
rel {def_a} 'DefBs'::'DefAs' {def_b}
```

Here, `def_a` has a property called `'DefBs'`, which is a relationship to one or more `def_b`s. Each `def_b` has a property called `'DefAs'`, which is a one or more `def_a`s.


