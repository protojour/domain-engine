# Generator types

***Generator types*** are used with the [`gen`](relation_types.md#gen) relationship type and arew used in relations whose target value may be automatically generated if no other value is given.

The following generator types are defined in the `ontol` domain:


## `auto`

Automatically generates a value if no value is given. Often used inline in a property relationship.

`auto` is valid for relations whose target type is [`serial`](primitives.md#serial), [`uuid`](primitives.md#uuid), or for [`fmt`](fmt.md) expressions that include them as the only variable.

```ontol
rel. 'id'[rel* gen: auto]: (rel* is: uuid)
```


## `create_time`

Generates a [`datetime`](primitives.md#datetime) **once**, when an entity is created.
Often used inline in a property relationship.

```ontol
rel* 'created'[rel* gen: create_time]?: datetime
```


## `update_time`

Generates a [`datetime`](primitives.md#datetime) **every time** an entity is updated.
Often used inline in a property relationship.

```ontol
rel* 'updated'[rel* gen: update_time]?: datetime
```
