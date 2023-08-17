# rel

A `rel` statement describes a relation, and has three parameters; the first is the subject, the second is the relation, and the third is the object.

The properties of a struct type are defined using relations:

```ontol
rel some_type 'relation_name': string
```

Here `some_type` is the subject, `'relation_name':` is the relation, and `string` is the object. In other words `some_type` has a property called `'relation_name'` which is a `string`.

## local rel

A common pattern is to define relations locally, within the block of a type. In this case, the subject can be replaced by a `.`.

```ontol
type some_type {
    rel . 'relation_name': string
}
```

This is the same as:

```ontol
type some_type
rel some_type 'relation_name': string
```

## one-to-one type relation

`rel` statements also describe relations between types. If the object is a struct type (i.e. not a simple scalar type like `string`),

```ontol
rel type_a 'TypeB'::'TypeA' type_b
```

## one-to-many type relation

Cardinality is indicated by enclosing the subject and/or object in square brackets.

```ontol
rel type_a 'TypeBs'::'TypeA' [type_b]
```

Here, `type_a` has a property called `'TypeBs'`, which is a relation to one or more `type_b`s. Each `type_b` has a property called `'TypeA'`, which is a relation to one `type_a`.

## many-to-many type relation

```ontol
rel [type_a] 'TypeBs'::'TypeAs' [type_b]
```

Here, `type_a` has a property called `'TypeBs'`, which is a relation to one or more `type_b`s. Each `type_b` has a property called `'TypeAs'`, which is a one or more `type_a`s.
