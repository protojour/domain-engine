use std::collections::{HashMap, hash_map::Entry};

use fnv::FnvHashMap;
use ontol_core::tag::OntolDefTag;
use ontol_syntax::rowan::GreenToken;

use crate::{
    log_model::ArcCoord,
    tables::{DomainTablesView, LayeredDomainTables},
    tag::Tag,
    token::Token,
};

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
    OntolModule(FnvHashMap<&'static str, SymEntry>),
    OntolDef(OntolDefTag),
}

impl SymEntry {
    pub fn local_def(&self) -> Option<Tag> {
        if let Self::LocalDef(tag) = self {
            Some(*tag)
        } else {
            None
        }
    }

    pub fn use_tag(&self) -> Option<Tag> {
        if let Self::Use(tag) = self {
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

impl<'a> LayeredDomainTables<'a> {
    pub fn new(back: DomainTablesView<'a>) -> Self {
        Self {
            front_symbols: Default::default(),
            front_edges: Default::default(),
            back,
        }
    }

    pub fn into_updates(self) -> SymbolTableUpdates {
        SymbolTableUpdates {
            symbols: self.front_symbols,
        }
    }

    pub fn remove_symbol(&mut self, symbol: &Token) -> Option<()> {
        self.front_symbols.insert(symbol.clone(), None);
        Some(())
    }

    pub fn put_symbol<T>(&mut self, symbol: Token, alloc: impl FnOnce() -> T) -> InsertResult<T>
    where
        T: Clone + Into<SymEntry> + TryFrom<SymEntry>,
    {
        let front_entry = self.front_symbols.entry(symbol.clone());

        match (front_entry, self.back.local.symbols.table.get(&symbol)) {
            (Entry::Vacant(vacant), None) => {
                let value = alloc();
                vacant.insert(Some(value.clone().into()));
                InsertResult::Inserted(value)
            }
            (Entry::Occupied(mut occ), None) => match occ.get() {
                Some(entry) => match T::try_from(entry.clone()) {
                    Ok(value) => InsertResult::ReusedFront(value),
                    Err(_) => InsertResult::Conflict,
                },
                None => {
                    let value = alloc();
                    occ.insert(Some(value.clone().into()));
                    InsertResult::Inserted(value)
                }
            },
            (Entry::Vacant(_vacant), Some(back)) => match T::try_from(back.clone()) {
                Ok(value) => InsertResult::ReusedBack(value),
                Err(_) => InsertResult::Conflict,
            },
            (Entry::Occupied(_occ), Some(_back)) => InsertResult::Conflict,
        }
    }

    pub fn put_edge_symbol(&mut self, edge_tag: Tag, symbol: Token, coord: ArcCoord) {
        self.front_edges
            .entry(edge_tag)
            .or_default()
            .symbols
            .insert(symbol, SymEntry::EdgeSymbol(edge_tag, coord));
    }
}
