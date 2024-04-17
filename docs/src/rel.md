# `rel`

The `rel` statement defines a ***relationship*** in the form of a _triplet_;

1. the first parameter is the _subject_,
2. the second is the _relation_, and
3. the third is the _object_

In ONTOL, just about everything is connected in some way via relationships.

The properties of a [`def`](def.md) are defined using relationships:

```ontol
rel subject 'relation_name': object
```

1. `subject` is the subject (the type for which the property is defined)
2. `'relation_name'` names the relation itself, and
3. `object` is the object (also a type)

In other words some `def subject` has a property called `'relation_name'`, which is some type called `object`.


## Optionality

A relation may be _optional_, denoted by a `?` in the relation (before the `:`):

```ontol
rel subject 'property'?: object
```

Here, the property `'property'` is optional, it is not required for a valid and complete `subject`.


## `.` ("self")

A common pattern is to define relationships within the block of a [`def`](def.md).

In the scope of any block, the subject or object can be replaced by a `.`, meaning _"self"_, referring to the enclosing type. It is usually used for the subject of a relation:

```ontol
def some_def (
    rel . 'property': text
)
```

This is the same as:

```ontol
def some_def ()
rel some_def 'property': text
```

ONTOL is lenient with spacing and line breaks, and the `.` is often contracted into the relation:

```ontol
def some_def (
    rel .'property': text
)
```


## `is` relationships

Not all relationships are named properties.

[`is`](relation_types.md#is) is a _relation type_ from the [`ontol` domain](ontol_domain.md) documented later, but it's such a central concept and invaluable basic building block, it is introduced here.

`is`, used as a relation, lets the subject type inherit all properties of the object type:

```ontol
def textlike (
    rel .is: text
)
```

`textlike` is now a _subtype_ of `text` – in ontology terms `textlike` _is-a_ `text`.

`textlike` has `text` as its _base type_, and will behave the same way for now, but it may also take on additional characteristics (properties, [doc comments](comments.md#doc-comments), etc.), as long as they are compatible.

`is` relationships have many uses, from field and structure re-use, inheritance and composition, to [unions](#unions) (below):


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

_Unions_ can be defined using conditional `is?` relationships.

A union type behaves as any other type, but represents one of several choices, its _variants_.

Union variants must have some discriminant, so that ONTOL can tell them apart; either their primary key structure is different or some inherent property is, such as a constant:

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

Unions of primitives or of literals (_enumerations_) are also possible, but union members all have to be of the same base type:

```ontol
def symbol_union (
    rel .is?: 'label_a'
    rel .is?: 'label_b'
)
```

## Flattened unions

Several union variants can be combined and flattened into a single type. This allows variants to be expressed, while preserving strong typing. To do this, an empty (unit) type is employed to "join" the variants into the parent type. The unit type can then be used to address the union variants in [`map`](map.md) expressions.

```ontol
def type ()

def foo (
    rel .'id'|id: (rel .is: text)
    rel .type: (
        rel .is?: type_a
        rel .is?: type_b
    )
)

def type_a (
    rel .'type': 'a'
    rel .'unique_to_a': text
)

def type_b (
    rel .'type': 'b'
    rel .'unique_to_b': text
)
```

Here, the field `unique_to_a` is only allowed (and required) if `type` is `'a'`, and `unique_to_b` is only allowed (and required) if `type` is `'b'`.


## Sequences

Both the subject and the object of a relationship may have multiple members, expressed as _sequences_.


### Sets

_Sets_ are expressed using `{}` parentheses. The members of a set are all required to be of the same type, and unique – there are no duplicates in a set.

```ontol
def some_def (
    rel .'texts': {text}
)
```

Here, the property `'texts'` is a set of `text`s.

Many things in ONTOL are represented as sets, including entity collections.

A value might be expressed e.g. `{'a', 'b', 'c'}`.


### Lists

_Lists_ are rarely used in ONTOL, but may be constructed using `[]` brackets. A list may be of any length, and may have duplicates.

```ontol
def some_def (
    rel .'texts': [text]
)
```

A value might be expressed e.g. `['a', 'b', 'c']` and its contents are adressed by index, `texts[0]`.


### Tuples

_Tuples_ are sequences of fixed length, and can be expressed using `rel` with indexes.

```ontol
def text_tuple (
    rel .0: text,
    rel .1: text,
)

def some_def (
    rel .'texts': text_tuple
)
```

Here, the type `text_tuple` is a pair of `text` values; no more, no less.

It could also have been expressed inline:

```ontol
def some_def (
    rel .'texts': (
        rel .0: text,
        rel .1: text,
    )
)
```

Tuples members can be of any type, but if its members are homogenous, its defintion can be simplified using a _range_ as shown below:

```ontol
def text_tuple (
    rel .0..2: text,
)
```

Ranges are A tuple's value might be expressed e.g. `['a', 'b']` and its contents are adressed by index, `text_tuple[0]`.


## Entity relationships

`rel` statements always describe relationships between `def`s, from subject to object. The subject and object are types, either primitives, structs, or certain special types.

If both the subject and object is an [entity](entities.md) (i.e. an identifiable type), their relationship is an ***entity relationship***, and the relationship between them are represented by edges in a graph.

```ontol
rel def_a 'relation': def_b
```

In a directed graph, the direction of the relationship is indicated by the order. The relationship goes from the subject to the object, or `def_a -> def_b`.


### One-to-one entity relationships

The simplest entity relationships are one-to-one relationships.

The relation can have different names on each end of the relationship. This is indicated by a `::` double colon operator:

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


### One-to-many entity relationships

_Cardinality_ is indicated by wrapping the subject and/or object as a [set](#sets):

```ontol
rel def_a 'DefBs'::'DefA' {def_b}
```

Here, `def_a` has a property called `'DefBs'`, which is a relationship to one or more `def_b`s. Each `def_b` has a property called `'DefA'`, which is a relationship to one `def_a`.


### Many-to-many entity relationships

```ontol
rel {def_a} 'DefBs'::'DefAs' {def_b}
```

Here, `def_a` has a property called `'DefBs'`, which is a relationship to one or more `def_b`s. Each `def_b` has a property called `'DefAs'`, which is a one or more `def_a`s.


## `rel` relationships and edge properties

A relation may have relationships of its own, indicated by a body delimited by square brackets `[]` within the relation parameter (before the `:`).

If there's only one `rel` statement, this is usually added inline, such as for [`default`](relation_types.md#default) and other relation types:

```ontol
def some_def (
    rel . 'active'[rel .default := true]: boolean
)
```

A relation may also "break out" its definition using an [`is`](#is-relationships) relation, useful for adding multiple edge properties:

```ontol
def person (
    rel .'id'|id: (rel .is: serial)
    rel .'best_friend'[rel .is: friendship]::'biggest_fan'? person
)

def friendship (
    rel .'start': datetime
    rel .'end'?: datetime
)
```

Multiple properties can also be added inline:

```ontol
def person (
    rel .'id'|id: (rel .is: serial)
    rel .'best_friend'[
        rel .'start': datetime
        rel .'end'?: datetime
    ]::'biggest_fan'? person
)
```

