use crate::{
    ontology::{domain::TypeKind, map::MapLossiness, Ontology},
    query::select::{EntitySelect, Select, StructOrUnionSelect},
    DefId, MapDef, MapDefFlags, MapFlags, MapKey, PackageId,
};
use fnv::{FnvHashMap, FnvHashSet};
use tracing::trace;

#[derive(Debug)]
pub struct ResolvePath {
    path: Vec<MapKey>,
}

impl ResolvePath {
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = MapKey> + '_ {
        self.path.iter().cloned()
    }

    pub fn reverse(&self) -> impl DoubleEndedIterator<Item = MapKey> + '_ {
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
    ByInput,
    ByOutput,
}

#[derive(Clone, Copy, Debug)]
pub enum ProbeFilter {
    /// The mapping must be complete/perfect, i.e. not lossy
    Complete,
    /// The mapping must be pure, i.e. not use the data store
    Pure,
}

#[derive(Debug)]
pub struct ResolverGraph {
    /// Graph of what each DefId can map to
    graph_by_input: FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>,
    /// Graph of what each DefId can map from
    graph_by_output: FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>,
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
                .map(|(key, meta)| (key, meta.lossiness)),
        )
    }

    pub fn new(iterator: impl Iterator<Item = (MapKey, MapLossiness)>) -> Self {
        let mut graph_by_input: FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>> =
            Default::default();
        let mut graph_by_output: FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>> =
            Default::default();

        for (key, lossiness) in iterator {
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

            graph_by_input
                .entry(key.input.def_id)
                .or_default()
                .entry(key.output)
                .or_default()
                .push(flags);

            graph_by_output
                .entry(key.output.def_id)
                .or_default()
                .entry(key.input)
                .or_default()
                .push(flags);
        }

        Self {
            graph_by_input,
            graph_by_output,
        }
    }

    pub fn probe_path_for_select(
        &self,
        ontology: &Ontology,
        select: &Select,
        target_package: PackageId,
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
                ontology,
                struct_select.def_id,
                target_package,
                ProbeOptions {
                    must_be_entity: true,
                    direction,
                    filter,
                },
            ),
            Select::Struct(struct_select) => self.probe_path(
                ontology,
                struct_select.def_id,
                target_package,
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
            Select::Leaf | Select::EntityId => Some(ResolvePath { path: vec![] }),
        }
    }

    pub fn probe_path_for_entity_select(
        &self,
        ontology: &Ontology,
        select: &EntitySelect,
        target_package: PackageId,
    ) -> Option<ResolvePath> {
        match &select.source {
            StructOrUnionSelect::Struct(struct_query) => self.probe_path(
                ontology,
                struct_query.def_id,
                target_package,
                ProbeOptions {
                    must_be_entity: true,
                    direction: ProbeDirection::ByOutput,
                    filter: ProbeFilter::Complete,
                },
            ),
            StructOrUnionSelect::Union(..) => todo!("Resolve a union"),
        }
    }

    pub fn probe_path(
        &self,
        ontology: &Ontology,
        public_def_id: DefId,
        target_package: PackageId,
        options: ProbeOptions,
    ) -> Option<ResolvePath> {
        let mut path = vec![];
        let mut visited = Default::default();
        let mut probe = Probe {
            ontology,
            graph: match options.direction {
                ProbeDirection::ByInput => &self.graph_by_input,
                ProbeDirection::ByOutput => &self.graph_by_output,
            },
            options: &options,
            path: &mut path,
            visited: &mut visited,
            target_package,
        };

        if probe.probe_rec(public_def_id) {
            Some(ResolvePath { path })
        } else {
            None
        }
    }
}

struct Probe<'on, 'a> {
    ontology: &'on Ontology,
    graph: &'on FnvHashMap<DefId, FnvHashMap<MapDef, Vec<Flags>>>,
    options: &'on ProbeOptions,
    path: &'a mut Vec<MapKey>,
    visited: &'a mut FnvHashSet<DefId>,
    target_package: PackageId,
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

        if def_id.package_id() == self.target_package {
            if self.options.must_be_entity {
                let type_info = self.ontology.get_type_info(def_id);
                if !matches!(&type_info.kind, TypeKind::Entity(_)) {
                    return false;
                }
            }

            return true;
        }

        let edges = match self.graph.get(&def_id) {
            Some(edges) => edges,
            None => return false,
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
                ProbeDirection::ByInput => {
                    self.path.push(MapKey {
                        input: MapDef {
                            def_id,
                            flags: MapDefFlags::empty(),
                        },
                        output: *edge_def,
                        flags: flags.map_flags,
                    });
                }
                ProbeDirection::ByOutput => {
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
