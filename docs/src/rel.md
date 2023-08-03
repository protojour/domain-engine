# rel

```ontol
rel some_type 'relation_name': string
```

## one-to-one type relation

```ontol
rel type_a 'TypeB'::'TypeA' type_b
```

## one-to-many type relation

```ontol
rel type_a 'TypeBees'::'TypeA' [type_b]
```

## many-to-many type relation

```ontol
rel [type_a] 'TypeBees'::'TypeAs' [type_b]
```

# local rel

```ontol
type some_type {
    rel . 'relation_name': string
}
```
