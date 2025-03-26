use std::collections::{BTreeMap, HashMap};

use either::Either;
use fnv::FnvHashMap;
use ontol_core::{
    property::ValueCardinality,
    tag::{OntolDefKind, OntolDefTag},
};
use ontol_parser::cst::inspect::{self as insp};
use ontol_syntax::syntax_view::RowanNodeView;

use crate::{
    analyzer::BlockContext,
    log_model::{TypeRef, TypeRefOrUnionOrPattern, TypeUnion},
    lookup::{LookupTable, resolve_pattern, resolve_type_reference},
    symbol::{SymEntry, SymbolTable, SymbolTableUpdates},
    tag::{LogRef, Tag},
    token::Token,
};

#[derive(Debug)]
pub struct GlobalTables {
    pub domains: FnvHashMap<(LogRef, Tag), DomainTables>,
    pub ontol: FnvHashMap<&'static str, SymEntry>,
}

impl Default for GlobalTables {
    fn default() -> Self {
        let mut ontol = FnvHashMap::<&'static str, SymEntry>::default();

        for tag in 0..u16::MAX {
            let Ok(ontol_def) = OntolDefTag::try_from(tag) else {
                break;
            };

            if ontol_def == OntolDefTag::_LastEntry {
                break;
            }

            let mut table = &mut ontol;
            let mut path = ontol_def.ontol_path().into_iter().copied().peekable();

            while let Some(segment) = path.next() {
                if path.peek().is_some() {
                    let next = table.get_mut(segment).unwrap();

                    table = match next {
                        SymEntry::OntolModule(module) => module,
                        e => panic!("{segment:?}: {e:?}"),
                    }
                } else {
                    table.insert(
                        segment,
                        match ontol_def.def_kind() {
                            Some(OntolDefKind::Module) => SymEntry::OntolModule(Default::default()),
                            Some(_) => SymEntry::OntolDef(ontol_def),
                            None => continue,
                        },
                    );
                }
            }
        }

        Self {
            domains: Default::default(),
            ontol,
        }
    }
}

#[derive(Default, Debug)]
pub struct DomainTables {
    pub symbols: SymbolTable,
    pub edges: FnvHashMap<Tag, EdgeMatrix>,
    pub rels: RelTable,
}

#[derive(Debug)]
pub struct LayeredDomainTables<'a> {
    pub front_symbols: HashMap<Token, Option<SymEntry>>,
    pub front_edges: HashMap<Tag, EdgeMatrix>,
    pub back: DomainTablesView<'a>,
}

#[derive(Debug)]
pub struct DomainTablesView<'a> {
    pub local: &'a DomainTables,
    pub global: &'a GlobalTables,
}

#[derive(Default, Debug)]
pub struct EdgeMatrix {
    pub symbols: FnvHashMap<Token, SymEntry>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RelKey {
    subject: Either<TypeRef, TypeUnion>,
    relation: (),
    object: TypeRefOrUnionOrPattern,
}

impl GlobalTables {
    pub fn domain_tables_mut(&mut self, log_ref: LogRef, domain_tag: Tag) -> &mut DomainTables {
        self.domains.entry((log_ref, domain_tag)).or_default()
    }

    pub fn domain_view(&mut self, log_ref: LogRef, domain_tag: Tag) -> DomainTablesView<'_> {
        self.domains.entry((log_ref, domain_tag)).or_default();
        let domain_table = self.domains.get(&(log_ref, domain_tag)).unwrap();

        DomainTablesView {
            local: domain_table,
            global: self,
        }
    }

    pub fn update(&mut self, log_ref: LogRef, domain_tag: Tag, updates: SymbolTableUpdates) {
        let domain_table = self.domains.entry((log_ref, domain_tag)).or_default();

        domain_table.symbols.update(updates);
    }
}

#[derive(Default, Debug)]
pub struct RelTable {
    pub table: BTreeMap<RelKey, Tag>,
}

impl RelKey {
    pub fn new(
        stmt: insp::RelStatement<RowanNodeView>,
        context: BlockContext,
        table: &impl LookupTable,
    ) -> Option<Self> {
        Some(Self {
            subject: resolve_type_reference(
                stmt.subject()?.type_quant()?.type_ref()?,
                &mut ValueCardinality::Unit,
                context,
                table,
            )?,
            relation: (),
            object: match stmt.object()?.type_quant_or_pattern()? {
                insp::TypeQuantOrPattern::TypeQuant(q) => resolve_type_reference(
                    q.type_ref()?,
                    &mut ValueCardinality::Unit,
                    context,
                    table,
                )?
                .into(),
                insp::TypeQuantOrPattern::Pattern(p) => {
                    TypeRefOrUnionOrPattern::Pattern(resolve_pattern(p, table)?)
                }
            },
        })
    }
}
