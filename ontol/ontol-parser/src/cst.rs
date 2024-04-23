//! Experimental CST-style parsing.
//!
//! CST means Concrete Syntax Tree.
//!
//! The difference with AST (abstract) is that the concrete syntax tree is non-lossy,
//! the original source code can be perfectly reconstructed from it.

// TODO: Remove this lint silencer
#![allow(unused)]

use std::{fmt::Display, io::Read, ops::Range};

use logos::Logos;

use crate::{lexer::kind::Kind, K};

use self::{
    parser::{CstParser, SyntaxCursor},
    tree::{DebugTree, FlatSyntaxTree, SyntaxNode},
};

pub mod grammar;
pub mod parser;
pub mod tree;
pub mod view;
