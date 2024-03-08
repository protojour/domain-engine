# Primitives

***Primitives*** are base scalar types of various levels of abstraction.

The following primitives are defined in the `ontol` domain:

## Unit

Unit is an explicit absence of value, similar to `null`, `nil` etc.

```ontol
rel .is: ()
```

## Numbers

```ontol
rel .is: -1
rel .is: 42
rel .is: 3.14
```

## Text literals

```ontol
rel .is: 'Text literal'
rel .is: "Also valid"
```

## Regular expressions

```ontol
rel .is: /\w+/
rel .is: /#[0-9a-fA-F]{6}/
```

{{#ontol-primitives}}
