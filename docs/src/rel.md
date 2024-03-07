# `rel`

The `rel` statement defines a ***relationship*** in the form of a _triplet_; the first parameter is the subject, the second is the relation, and the third is the object.

The properties of a struct [`def`](def.md) are defined using relationships:

```ontol
rel some_def 'relation_name': text
```

Here `some_def` is the subject, `'relation_name':` names the relation itself, and `text` is the object. In other words `some_def` has a property called `'relation_name'` which is a [`text`](primitives.md#text).

A relation may be _optional_, denoted by a `?` in the relation:

```ontol
rel some_def 'relation_name'?: text
```


## Local `rel`

A common pattern is to define relationships locally, within the block of a [`def`](def.md). In this case, the subject can be replaced by a `.`, meaning "self".

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


## Sets

Sets are expressed using `{}` parenteses.

```ontol
def some_def (
    rel . 'texts': {text}
)
```

Here, the property `'texts'` is a set of `text`s (plural).


## One-to-one entity relationship

`rel` statements always describe relationships between `def`s. If the object is an [entity](entities.md) (i.e. not a simple struct, or a primitive def like [`text`](primitives.md#text)), the relation can have different names on each end of the relationship. This is indicated by a `::` double colon operator:

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
