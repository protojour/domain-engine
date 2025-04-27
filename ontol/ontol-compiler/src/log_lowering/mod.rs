use std::collections::BTreeSet;

use fnv::FnvHashMap;
use ontol_core::{
    LogRef,
    property::ValueCardinality,
    url::{DomainUrl, DomainUrlParser},
};
use ontol_log::{log_model::ValueCrd, sem_model::GlobalSemModel, sem_stmts::Rel, tag::Tag};
use ontol_parser::{
    ParserError,
    source::{SourceId, SourceSpan},
    topology::WithDocs,
};
use ontol_runtime::{DefId, DomainIndex};

use crate::{
    Compiler,
    lowering::context::LoweringOutcome,
    relation::{RelId, Relationship},
};

mod lower_arc;
mod lower_def;
mod lower_pattern;
mod lower_rel;
mod lower_sym;
mod type_ref;

pub struct LogLowering<'c, 'm, 'l> {
    compiler: &'c mut Compiler<'m>,
    global_model: &'l GlobalSemModel,
    local_log: LogRef,
    subdomain: Tag,
    #[expect(unused)]
    url: DomainUrl,
    subdomain_def_id: DefId,
    domain_index: DomainIndex,
    source_id: SourceId,
    #[expect(unused)]
    anonymous_unions: FnvHashMap<BTreeSet<DefId>, DefId>,
    outcome: LoweringOutcome,
    rel_by_parent: FnvHashMap<Tag, Vec<(Tag, &'l Rel)>>,
    #[expect(unused)]
    domain_url_parser: DomainUrlParser,
}

enum TypeContext {
    Root,
    RelParams {
        rel_id: RelId,
        rel_def_id: Option<DefId>,
        modifiers: Vec<(Relationship, SourceSpan)>,
    },
}

impl<'c, 'm, 'l> LogLowering<'c, 'm, 'l> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: DomainUrl,
        domain_def_id: DefId,
        source_id: SourceId,
        domain_index: DomainIndex,
        local_log: LogRef,
        domain_tag: Tag,
        global_model: &'l GlobalSemModel,
        compiler: &'c mut Compiler<'m>,
    ) -> Self {
        Self {
            compiler,
            global_model,
            subdomain: domain_tag,
            local_log,
            url,
            subdomain_def_id: domain_def_id,
            domain_index,
            source_id,
            anonymous_unions: Default::default(),
            outcome: Default::default(),
            rel_by_parent: Default::default(),
            domain_url_parser: Default::default(),
        }
    }

    pub fn lower(&mut self) {
        {
            let mut parse_errors: Vec<ParserError> = vec![];
            let header_data = self.global_model.subdomain_header_data(
                self.local_log,
                self.subdomain,
                WithDocs(true),
                &mut parse_errors,
            );
            self.outcome.header_data = Some(header_data);
        }

        let Some(model) = self.global_model.get_model(self.local_log) else {
            return;
        };

        for (tag, def) in &model.defs {
            if def.subdomain == Some(self.subdomain) {
                self.lower_def(*tag, def);
            }
        }

        for (tag, arc) in &model.arcs {
            if arc.subdomain == Some(self.subdomain) {
                self.lower_arc(*tag, arc);
            }
        }

        {
            let mut root_rels = vec![];

            for (tag, rel) in &model.rels {
                if rel.subdomain == Some(self.subdomain) {
                    if let Some(parent) = rel.parent_rel {
                        self.rel_by_parent
                            .entry(parent)
                            .or_default()
                            .push((*tag, rel));
                    } else {
                        root_rels.push((*tag, rel));
                    }
                }
            }

            for (tag, rel) in root_rels {
                self.lower_rel(tag, rel, &mut TypeContext::Root);
            }
        }
    }
}

fn map_sem_value_crd(cardinality: ValueCrd) -> ValueCardinality {
    match cardinality {
        ValueCrd::Unit => ValueCardinality::Unit,
        ValueCrd::Set => ValueCardinality::IndexSet,
        ValueCrd::List => ValueCardinality::List,
    }
}
