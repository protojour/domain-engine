# Introduction

ONTOL (ONTOlogy Language) is a declarative language for ***domain modelling***; expressing abstract and concrete concepts, data types and data structures, how they relate to other concepts, and how structures map to one another. It allows you to develop and document an ***ontology*** from its most basic roots, which can then be compiled and interacted with through the Memoriam domain engine.

ONTOL is not a general-purpose programming language, but has powerful features for transforming data from one form to another, and can be extended with custom logic.

What does it look like? Here's a small subset representation of [GeoJSON](https://en.wikipedia.org/wiki/GeoJSON) to illustrate:

```ontol
/// Longitude lines run north-south. They converge at the poles.
/// Values range between -180 and +180 degrees, with 0 at Greenwich.
/// Positive values are east, negative values are west
def longitude (
    rel* is: number
    rel* min: -180
    rel* max: 180
)

/// Latitude lines run east-west and are parallel to each other.
/// Values range between -90 and +90 degrees, with 0 at the equator.
/// Positive values are north, negative values are south.
def latitude (
    rel* is: number
    rel* min: -90
    rel* max: 90
)


/// A position is the fundamental geometry construct, represented as a tuple.
def position (
    rel* 0: longitude
    rel* 1: latitude
)

/// For type "Point", the "coordinates" member is a single position.
def Point (
    rel* 'type': 'Point'
    rel* 'coordinates': position
)
```

Fundamental constructs and concepts in ONTOL are gradually introduced in the **ONTOL Reference**, starting with [_definitions_](def.md) (`def`, above).

You might want to read up on some [theory and background](theory_and_background.md) in the next chapter first.
