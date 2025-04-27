use std::collections::HashMap;

use fnv::FnvHashMap;
use ontol_core::tag::OntolDefTag;
use ontol_syntax::rowan::GreenToken;

use crate::{log_model::ArcCoord, tag::Tag, token::Token};

#[derive(Default, Debug)]
pub struct SymbolTable {
    pub table: HashMap<Token, SymEntry>,
}

pub struct SymbolTableUpdates {
    symbols: HashMap<Token, Option<SymEntry>>,
}

impl SymbolTableUpdates {
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }
}

#[derive(Clone, Debug)]
pub enum SymEntry {
    Use(Tag),
    LocalDef(Tag),
    LocalEdge(Tag),
    EdgeSymbol(Tag, ArcCoord),
    Ontol,
    OntolModule(FnvHashMap<&'static str, SymEntry>),
    OntolDef(OntolDefTag),
}

impl SymEntry {
    pub fn entry_ref(&self) -> SymEntryRef<'_> {
        match self {
            SymEntry::Use(tag) => SymEntryRef::Use(*tag),
            SymEntry::LocalDef(tag) => SymEntryRef::LocalDef(*tag),
            SymEntry::LocalEdge(tag) => SymEntryRef::LocalEdge(*tag),
            SymEntry::EdgeSymbol(tag, coord) => SymEntryRef::EdgeSymbol(*tag, *coord),
            SymEntry::Ontol => SymEntryRef::Ontol,
            SymEntry::OntolModule(table) => SymEntryRef::OntolModule(table),
            SymEntry::OntolDef(tag) => SymEntryRef::OntolDef(*tag),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SymEntryRef<'a> {
    Use(Tag),
    LocalDef(Tag),
    LocalEdge(Tag),
    EdgeSymbol(Tag, ArcCoord),
    Ontol,
    OntolModule(&'a FnvHashMap<&'static str, SymEntry>),
    OntolDef(OntolDefTag),
}

impl SymEntryRef<'_> {
    pub fn use_tag(&self) -> Option<Tag> {
        if let Self::Use(tag) = self {
            Some(*tag)
        } else {
            None
        }
    }

    pub fn local_def(&self) -> Option<Tag> {
        if let Self::LocalDef(tag) = self {
            Some(*tag)
        } else {
            None
        }
    }

    pub fn local_arc(&self) -> Option<Tag> {
        if let Self::LocalEdge(tag) = self {
            Some(*tag)
        } else {
            None
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SymbolError {
    #[error("redefinition of symbol")]
    AlreadyDefined,
}

pub enum InsertResult<T> {
    Inserted(T),
    ReusedFront(T),
    ReusedBack(T),
    Conflict,
}

impl<T> InsertResult<T> {
    pub fn inserted(self) -> Option<T> {
        match self {
            Self::Inserted(t) | Self::ReusedFront(t) => Some(t),
            Self::ReusedBack(_) | Self::Conflict => None,
        }
    }
}

impl SymbolTable {
    pub fn put_symbol(&mut self, symbol: GreenToken, entry: SymEntry) -> Result<(), SymbolError> {
        if let Some(_old) = self.table.insert(Token(symbol), entry) {
            Err(SymbolError::AlreadyDefined)
        } else {
            Ok(())
        }
    }

    pub fn update(&mut self, updates: SymbolTableUpdates) {
        for (key, val) in updates.symbols {
            match val {
                Some(entry) => {
                    self.table.insert(key, entry);
                }
                None => {
                    self.table.remove(&key);
                }
            }
        }
    }
}
