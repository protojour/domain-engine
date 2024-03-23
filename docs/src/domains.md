# Domains

A collection of related concepts is known as a ***domain***.

Every ONTOL (`.on`) file describes a single domain. Domains may depend on other domains through [`use` imports](use.md). Some domains may be wholly abstract or define the axioms of the universe, some are concrete, pragmatic, or built purely for data processing, others again may be full of utility structures and help keep your core domains clean.

Here's a simple example domain:

```ontol
use 'world' as world

def book (
    rel .'id'|id: (rel .is: serial)
    rel .'title': text
    rel .'editions': {edition}
    rel {.} 'author': {author}
)

def author (
    rel .'id'|id: (rel .is: serial)
    rel .'name': text
    rel .'nationality': world.country
    rel .'born': datetime
    rel .'died'?: datetime
)

def edition (
    rel .'id'|id: (rel .is: serial)
    rel .'publisher': publisher
    rel .'published': datetime
    rel .'isbn': text
    rel .'pages': i64
)

def publisher (
    rel .'id'|id: (rel .is: serial)
    rel .'name': text
    rel .'headquarters': world.city
)
```

Notice how `world.country` and `world.city` are borrowed from a different domain. We wouldn't want to include _every_ tangentially related concept in our "books" domain, as countries and cities have very little to do with books.

Domains are reusable, shareable, exchangable, and get better the more you work on them.

Remember to document the domain using [doc comments](comments.md#doc-comments).


## Ontologies

A collection of domains is called an ***ontology*** (from greek; _'the study of being'_ or of _'that which is'_), and represents a complete world view – for a given use case. While many formal ontologies actually try to represent a _complete_ world view, it probably is not a requirement in yours.

The ONTOL compiler may take in many domains, and the binary produces is also called an ontology.
