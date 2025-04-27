use fnv::FnvHashMap;
use ontol_core::{
    property::ValueCardinality,
    tag::{OntolDefKind, OntolDefTag},
};
use ontol_parser::cst::inspect::{self as insp};
use ontol_syntax::syntax_view::RowanNodeView;

use crate::{
    analyzer::BlockContext,
    error::{OptionExt, SemError},
    log_model::{TypeRef, TypeRefOrUnionOrPattern, TypeUnion},
    lookup::{LookupTable, resolve_pattern, resolve_type_reference},
    symbol::SymEntry,
    tag::Tag,
};

pub fn ontol_table() -> FnvHashMap<&'static str, SymEntry> {
    let mut ontol = FnvHashMap::<&'static str, SymEntry>::default();

    for tag in 0..u16::MAX {
        let Ok(ontol_def) = OntolDefTag::try_from(tag) else {
            break;
        };

        if ontol_def == OntolDefTag::_LastEntry {
            break;
        }

        let mut table = &mut ontol;
        let mut path = ontol_def.ontol_path().iter().copied().peekable();

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

    ontol
}

/// A relation key is local to one log (is valid for all the domains in the log).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RelKey {
    pub parent_rel: Option<Tag>,
    pub subject: TypeUnion,
    pub relation: TypeRef,
    pub object: TypeRefOrUnionOrPattern,
}

impl RelKey {
    pub fn new(
        stmt: insp::RelStatement<RowanNodeView>,
        context: BlockContext,
        table: &impl LookupTable,
    ) -> Result<Self, SemError> {
        Ok(Self {
            parent_rel: None,
            subject: resolve_type_reference(
                stmt.subject()
                    .stx_err()?
                    .type_quant()
                    .stx_err()?
                    .type_ref()
                    .stx_err()?,
                &mut ValueCardinality::Unit,
                context,
                table,
            )?
            .right_or_else(|t| [t].into_iter().collect()),
            relation: resolve_type_reference(
                stmt.relation()
                    .stx_err()?
                    .relation_type()
                    .stx_err()?
                    .type_ref()
                    .stx_err()?,
                &mut ValueCardinality::Unit,
                context,
                table,
            )?
            .left()
            .stx_err()?
            .0,
            object: match stmt.object().stx_err()?.type_quant_or_pattern().stx_err()? {
                insp::TypeQuantOrPattern::TypeQuant(q) => resolve_type_reference(
                    q.type_ref().stx_err()?,
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
