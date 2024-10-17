use crate::{
    ontology::{aspects::DefsAspect, domain::DefKind, map::MapLossiness, Ontology},
    query::select::{EntitySelect, Select, StructOrUnionSelect},
    DefId, MapDef, MapDefFlags, MapDirection, MapFlags, MapKey,
};
use fnv::{FnvHashMap, FnvHashSet};
use tracing::trace;

#[derive(Clone, Default, Debug)]
pub struct ResolvePath {
    path: Vec<MapKey>,
}

impl ResolvePath {
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = MapKey> + use<'_> {
        self.path.iter().cloned()
    }

    pub fn reverse(&self) -> impl DoubleEndedIterator<Item = MapKey> + use<'_> {
        self.path.iter().cloned().rev().map(|key| MapKey {
            input: key.output,
            output: key.input,
            flags: key.flags,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProbeOptions {
    pub must_be_entity: bool,
    pub direction: ProbeDirection,
    pub filter: ProbeFilter,
}

#[derive(Clone, Copy, Debug)]
pub enum ProbeDirection {
    /// Probe for downmappings, i.e. data flowing towards persistence layer
    Down,
    /// Probe for upmappings, i.e. data flowing out of persistence layer
    Up,
}

#[derive(Clone, Copy, Debug)]
pub enum ProbeFilter {
    /// The mapping must be complete/perfect, i.e. not lossy
    Complete,
    /// The mapping must be pure, i.e. not use the data store
    Pure,
}

type Graph = FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>;

#[derive(Debug, Default)]
pub struct ResolverGraph {
    /// Graph of mappings pointing down, keyed by input.
    /// A search starts at the top
    downgraph: FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>,
    /// Graph of mappings pointing up, keyed by output.
    /// A search starts at the top also in this graph.
    upgraph: FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>,
}

#[derive(Clone, Copy, Debug)]
struct Flags {
    map_flags: MapFlags,
    lossiness: MapLossiness,
}

impl ResolverGraph {
    pub fn from_ontology(ontology: &Ontology) -> Self {
        Self::new(
            ontology
                .iter_map_meta()
                .map(|(key, meta)| (key, meta.direction, meta.lossiness)),
        )
    }

    pub fn new(iterator: impl Iterator<Item = (MapKey, MapDirection, MapLossiness)>) -> Self {
        let mut downgraph = Graph::default();
        let mut upgraph = Graph::default();

        for (key, direction, lossiness) in iterator {
            let flags = Flags {
                lossiness,
                map_flags: key.flags,
            };

            trace!(
                "Add map pair {:?} {:?} {:?}",
                key.input.def_id,
                key.output.def_id,
                key.flags
            );

            match direction {
                MapDirection::Down => {
                    downgraph
                        .entry(key.input.def_id)
                        .or_default()
                        .entry(key.output)
                        .or_default()
                        .push(flags);
                }
                MapDirection::Up => {
                    upgraph
                        .entry(key.output.def_id)
                        .or_default()
                        .entry(key.input)
                        .or_default()
                        .push(flags);
                }
                MapDirection::Mixed => {}
            }
        }

        Self { downgraph, upgraph }
    }

    pub fn probe_path_for_select(
        &self,
        ontology_defs: &DefsAspect,
        select: &Select,
        direction: ProbeDirection,
        filter: ProbeFilter,
    ) -> Option<ResolvePath> {
        match select {
            Select::Entity(
                EntitySelect {
                    source: StructOrUnionSelect::Struct(struct_select),
                    ..
                },
                ..,
            ) => self.probe_path(
                ontology_defs,
                struct_select.def_id,
                ProbeOptions {
                    must_be_entity: true,
                    direction,
                    filter,
                },
            ),
            Select::Struct(struct_select) => self.probe_path(
                ontology_defs,
                struct_select.def_id,
                ProbeOptions {
                    must_be_entity: true,
                    direction,
                    filter,
                },
            ),
            Select::StructUnion(..) => {
                todo!()
            }
            Select::Entity(
                EntitySelect {
                    source: StructOrUnionSelect::Union(..),
                    ..
                },
                ..,
            ) => {
                todo!()
            }
            Select::Unit | Select::EntityId | Select::VertexAddress => {
                Some(ResolvePath { path: vec![] })
            }
        }
    }

    pub fn probe_path_for_entity_select(
        &self,
        ontology_defs: &DefsAspect,
        select: &EntitySelect,
    ) -> Option<ResolvePath> {
        match &select.source {
            StructOrUnionSelect::Struct(struct_query) => self.probe_path(
                ontology_defs,
                struct_query.def_id,
                ProbeOptions {
                    must_be_entity: true,
                    direction: ProbeDirection::Up,
                    filter: ProbeFilter::Complete,
                },
            ),
            StructOrUnionSelect::Union(..) => todo!("Resolve a union"),
        }
    }

    pub fn probe_path(
        &self,
        ontology_defs: &DefsAspect,
        public_def_id: DefId,
        options: ProbeOptions,
    ) -> Option<ResolvePath> {
        let mut path = vec![];
        let mut visited = Default::default();
        let mut probe = Probe {
            ontology_defs,
            graph: match options.direction {
                ProbeDirection::Down => &self.downgraph,
                ProbeDirection::Up => &self.upgraph,
            },
            options: &options,
            path: &mut path,
            visited: &mut visited,
        };

        if probe.probe_rec(public_def_id) {
            Some(ResolvePath { path })
        } else {
            None
        }
    }

    pub fn is_mapped_down(&self, def_id: &DefId) -> bool {
        self.downgraph.contains_key(def_id)
    }

    pub fn is_mapped_up(&self, def_id: &DefId) -> bool {
        self.upgraph.contains_key(def_id)
    }

    pub fn down_persistence_terminals(&self) -> FnvHashSet<DefId> {
        cross_domain_edge_counts(&self.downgraph)
            .into_iter()
            .filter(|(_, count)| *count == 0)
            .map(|(def_id, _)| def_id)
            .collect()
    }

    pub fn up_persistence_terminals(&self) -> FnvHashSet<DefId> {
        cross_domain_edge_counts(&self.upgraph)
            .into_iter()
            .filter(|(_, count)| *count == 0)
            .map(|(def_id, _)| def_id)
            .collect()
    }
}

struct Probe<'on, 'a> {
    ontology_defs: &'on DefsAspect,
    graph: &'on FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>,
    options: &'on ProbeOptions,
    path: &'a mut Vec<MapKey>,
    visited: &'a mut FnvHashSet<DefId>,
}

impl<'on, 'a> Probe<'on, 'a> {
    fn probe_rec(&mut self, def_id: DefId) -> bool {
        trace!(
            "probe {def_id:?} path={:?} dir={:?}",
            self.path,
            self.options.direction
        );

        if !self.visited.insert(def_id) {
            return false;
        }

        let edges = match self.graph.get(&def_id) {
            Some(edges) => edges,
            None => {
                if self.options.must_be_entity {
                    let def = self.ontology_defs.def(def_id);
                    if !matches!(&def.kind, DefKind::Entity(_)) {
                        return false;
                    }
                }

                return true;
            }
        };

        for (edge_def, flag_candidates) in edges {
            let flags = match self.options.filter {
                ProbeFilter::Complete => flag_candidates
                    .iter()
                    .find(|flags| !flags.map_flags.contains(MapFlags::PURE_PARTIAL)),
                ProbeFilter::Pure => {
                    flag_candidates
                        .iter()
                        .find(|flags| flags.map_flags.contains(MapFlags::PURE_PARTIAL))
                        // Fall back to the default flag if there's no PURE PARTIAL flags
                        .or_else(|| flag_candidates.iter().next())
                }
            };

            let Some(flags) = flags else {
                continue;
            };

            if matches!(self.options.filter, ProbeFilter::Complete)
                && !matches!(flags.lossiness, MapLossiness::Complete)
            {
                continue;
            }

            match self.options.direction {
                ProbeDirection::Down => {
                    self.path.push(MapKey {
                        input: MapDef {
                            def_id,
                            flags: MapDefFlags::empty(),
                        },
                        output: *edge_def,
                        flags: flags.map_flags,
                    });
                }
                ProbeDirection::Up => {
                    self.path.push(MapKey {
                        input: *edge_def,
                        output: MapDef {
                            def_id,
                            flags: MapDefFlags::empty(),
                        },
                        flags: flags.map_flags,
                    });
                }
            }

            if self.probe_rec(edge_def.def_id) {
                return true;
            } else {
                self.path.pop();
            }
        }

        false
    }
}

fn cross_domain_edge_counts(graph: &Graph) -> FnvHashMap<DefId, u32> {
    let mut counts: FnvHashMap<DefId, u32> = FnvHashMap::default();

    for (input, edges) in graph {
        let count = counts.entry(*input).or_default();

        if edges
            .iter()
            .any(|(map_def, _)| map_def.def_id.domain_index() != input.domain_index())
        {
            *count += 1;
        }
    }

    trace!("edge_counts: {counts:#?}");

    counts
}
