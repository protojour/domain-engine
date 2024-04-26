//! Experimental CST-style parsing.
//!
//! CST means Concrete Syntax Tree.
//!
//! The difference with AST (abstract) is that the concrete syntax tree is non-lossy,
//! the original source code can be perfectly reconstructed from it.

pub mod grammar;
pub mod inspect;
pub mod parser;
pub mod tree;
pub mod view;
