use crate::{
    ontology::{MapLossiness, Ontology},
    select::{EntitySelect, Select, StructOrUnionSelect},
    DefId, MapDef, MapDefFlags, MapFlags, MapKey, PackageId,
};
use fnv::{FnvHashMap, FnvHashSet};
use tracing::{debug, trace};

#[derive(Debug)]
pub struct ResolvePath {
    path: Vec<MapKey>,
}

impl ResolvePath {
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = MapKey> + '_ {
        self.path.iter().cloned()
    }

    pub fn reverse(&self) -> impl Iterator<Item = MapKey> + '_ {
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

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug)]
    pub struct ProbeFilter: u8 {
        /// The mapping must be perfect, i.e. not lossy
        const PERFECT         = 0b00000001;
        const PURE            = 0b00000010;
    }
}

#[derive(Debug)]
pub struct ResolverGraph {
    /// Graph of what each DefId can map to
    graph_by_input: FnvHashMap<DefId, Vec<(MapDef, Flags)>>,
    /// Graph of what each DefId can map from
    graph_by_output: FnvHashMap<DefId, Vec<(MapDef, Flags)>>,
}

#[derive(Clone, Copy, Debug)]
struct Flags {
    key_flags: MapFlags,
    lossiness: MapLossiness,
}

impl ResolverGraph {
    pub fn from_ontology(ontology: &Ontology) -> Self {
        Self::new(
            ontology
                .iter_map_meta()
                .map(|(keys, meta)| (keys, meta.lossiness)),
        )
    }

    pub fn new(iterator: impl Iterator<Item = (MapKey, MapLossiness)>) -> Self {
        let mut graph_by_input: FnvHashMap<DefId, Vec<(MapDef, Flags)>> = Default::default();
        let mut graph_by_output: FnvHashMap<DefId, Vec<(MapDef, Flags)>> = Default::default();

        for (key, lossiness) in iterator {
            let flags = Flags {
                lossiness,
                key_flags: key.flags,
            };

            debug!(
                "Add map pair {:?} {:?}",
                key.input.def_id, key.output.def_id
            );

            graph_by_input
                .entry(key.input.def_id)
                .or_default()
                .push((key.output, flags));

            assert!(graph_by_input.contains_key(&key.input.def_id));

            graph_by_output
                .entry(key.output.def_id)
                .or_default()
                .push((key.input, flags));
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
                    direction: ProbeDirection::ByOutput,
                    filter: ProbeFilter::empty(),
                },
            ),
            Select::Struct(struct_select) => self.probe_path(
                ontology,
                struct_select.def_id,
                target_package,
                ProbeOptions {
                    must_be_entity: true,
                    direction: ProbeDirection::ByOutput,
                    filter: ProbeFilter::empty(),
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
                    filter: ProbeFilter::PERFECT,
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
    graph: &'on FnvHashMap<DefId, Vec<(MapDef, Flags)>>,
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

        if self.options.must_be_entity {
            let type_info = self.ontology.get_type_info(def_id);
            if type_info.entity_info.is_none() {
                return false;
            }
        }

        if def_id.package_id() == self.target_package {
            return true;
        }

        let edges = match self.graph.get(&def_id) {
            Some(edges) => edges,
            None => return false,
        };

        for (edge_key, flags) in edges {
            if self.options.filter.contains(ProbeFilter::PERFECT)
                && !matches!(flags.lossiness, MapLossiness::Perfect)
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
                        output: *edge_key,
                        flags: flags.key_flags,
                    });
                }
                ProbeDirection::ByOutput => {
                    self.path.push(MapKey {
                        input: *edge_key,
                        output: MapDef {
                            def_id,
                            flags: MapDefFlags::empty(),
                        },
                        flags: flags.key_flags,
                    });
                }
            }

            if self.probe_rec(edge_key.def_id) {
                return true;
            } else {
                self.path.pop();
            }
        }

        false
    }
}
