# Primitives

***Primitives*** are base scalar (non-composite) types defined within the `'ontol'` domain. They may be used directly, or be used as the basis for other types using inheritance through [`is` relationships](rel.md#is-relationships).

Some primitives are ***abstract***, meaning they are fine for defining types as long as they are not used in [data store](data_stores.md)-backed domains or [interfaces](interfaces.md). A type may e.g. inherit from `number`, but in order to be serialized or deserialized to a data store or interface, it needs to be _representable_ in a numeric format; it needs to have a concrete type, such as `i64` or `f64`.

***Literals*** are any constant value, part of the language itself.

## Unit

Unit is a type with an explicit absence of value, similar to `null`, `nil` etc.

```ontol
()
```

## Number literals

Number literals can be [integers](#integer) or [floats](#float).

```ontol
-1    // negative integer
42    // positive integer
3.14  // float
```

## Text literals

Text literals are expressed using `'single quotes'` or `"double quoutes"`.

```ontol
'Text literal'
"Also valid"
```

## Regular expressions

Regular expressions are enclosed by `/slashes/`.

In their simplest form, they express variable text values conforming to some pattern. Further use is detailed in the chapter on [patterns](fmt.md#regular-expressions) and [mappings](map.md#map-with-regular-expressions).

```ontol
/./                // any single character
/\w+/              // one or more "word" characters
/#[0-9a-fA-F]{6}/  // RGB hexadecimal color code, e.g. #e6db74
```

{{#ontol-primitives}}
