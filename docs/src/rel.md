# rel

The `rel` statement defines a relation, and has three parameters; the first is the subject, the second is the relation itself, and the third is the object.

The properties of a struct [`def`](def.md) are defined using relations:

```ontol
rel some_def 'relation_name': text
```

Here `some_def` is the subject, `'relation_name':` names the relation itself, and `text` is the object. In other words `some_def` has a property called `'relation_name'` which is a `text`.

## local rel

A common pattern is to define relations locally, within the block of a def. In this case, the subject can be replaced by a `.`.

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

## one-to-one def relation

`rel` statements always describe relations between `def`s. If the object is a struct def (i.e. not a simple scalar def like `text`), the relation can have different names on each end of the relation. This is indicated by a `::` double colon operator:

```ontol
rel def_a 'DefB'::'DefA' def_b
```

In a directed graph, the direction of the relation is indicated by the order. The relation goes from the subject to the object, or `def_a -> def_b`.

## one-to-many def relation

Cardinality is indicated by enclosing the subject and/or object in curly braces.

```ontol
rel def_a 'DefBs'::'DefA' {def_b}
```

Here, `def_a` has a property called `'DefBs'`, which is a relation to one or more `def_b`s. Each `def_b` has a property called `'DefA'`, which is a relation to one `def_a`.

## many-to-many def relation

```ontol
rel {def_a} 'DefBs'::'DefAs' {def_b}
```

Here, `def_a` has a property called `'DefBs'`, which is a relation to one or more `def_b`s. Each `def_b` has a property called `'DefAs'`, which is a one or more `def_a`s.
