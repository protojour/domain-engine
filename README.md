# ONTOL

A proof of concept domain description and mapping language.

The name is a silly spin on ALGOL (for algorithms!), this is a language for ontologies.

We'll try to keep things simple where we can.
The simplest syntactical "framework" is S-expressions (with a bracket extension) so that's what we'll at least start with.

## Language fundamentals

Work in progress. See [MDP 6](https://gitlab.com/protojour/x-design-proposals/-/issues/8).

## Architecture

Ideal high level data flow:

1. Create _compiler_. The compiler is single threaded.
2. Feed domain source code into the compiler.
3. The compiler finally produces a shared, immutable and thread safe _environment_.
4. Execute code by creating a _virtual machine_ which holds a reference to the program.

Any number of virtual machines may be created and executed in parallel, because of the immutable environment.

In the future, we may support precompiled environments.
