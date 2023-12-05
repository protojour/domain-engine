use crate::{
    ontology::{MapLossiness, Ontology},
    select::{EntitySelect, Select, StructOrUnionSelect},
    DefId, MapKey, PackageId,
};
use fnv::{FnvHashMap, FnvHashSet};
use tracing::trace;

#[derive(Debug)]
pub struct ResolvePath {
    path: Vec<(DefId, DefId)>,
}

impl ResolvePath {
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (DefId, DefId)> + '_ {
        self.path.iter().cloned()
    }

    pub fn reverse(&self) -> impl Iterator<Item = (DefId, DefId)> + '_ {
        self.path.iter().cloned().rev().map(|(from, to)| (to, from))
    }
}

#[derive(Clone, Copy)]
pub struct ProbeOptions {
    pub must_be_entity: bool,
    pub inverted: bool,
    pub filter_lossiness: Option<MapLossiness>,
}

pub struct ResolverGraph {
    /// Graph of what each DefId can map to
    map_graph: FnvHashMap<MapKey, Vec<(MapLossiness, MapKey)>>,
    /// Graph of what each DefId can map from
    inverted_map_graph: FnvHashMap<MapKey, Vec<(MapLossiness, MapKey)>>,
}

impl ResolverGraph {
    pub fn new(ontology: &Ontology) -> Self {
        Self::from_iter(
            ontology
                .iter_map_meta()
                .map(|(keys, meta)| (keys, meta.lossiness)),
        )
    }

    pub fn from_iter(iterator: impl Iterator<Item = ([MapKey; 2], MapLossiness)>) -> Self {
        let mut map_graph: FnvHashMap<MapKey, Vec<(MapLossiness, MapKey)>> = Default::default();
        let mut inverted_map_graph: FnvHashMap<MapKey, Vec<(MapLossiness, MapKey)>> =
            Default::default();

        for ([source_key, target_key], map_kind) in iterator {
            map_graph
                .entry(source_key)
                .or_default()
                .push((map_kind, target_key));
            inverted_map_graph
                .entry(target_key)
                .or_default()
                .push((map_kind, source_key));
        }

        Self {
            map_graph,
            inverted_map_graph,
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
                    inverted: true,
                    filter_lossiness: None,
                },
            ),
            Select::Struct(struct_select) => self.probe_path(
                ontology,
                struct_select.def_id,
                target_package,
                ProbeOptions {
                    must_be_entity: true,
                    inverted: true,
                    filter_lossiness: None,
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
                    inverted: true,
                    filter_lossiness: None,
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
            graph: if options.inverted {
                &self.inverted_map_graph
            } else {
                &self.map_graph
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
    graph: &'on FnvHashMap<MapKey, Vec<(MapLossiness, MapKey)>>,
    options: &'on ProbeOptions,
    path: &'a mut Vec<(DefId, DefId)>,
    visited: &'a mut FnvHashSet<DefId>,
    target_package: PackageId,
}

impl<'on, 'a> Probe<'on, 'a> {
    fn probe_rec(&mut self, def_id: DefId) -> bool {
        trace!("probe {def_id:?} path={:?}", self.path);

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

        let key = MapKey { def_id, seq: false };
        let edges = match self.graph.get(&key) {
            Some(edges) => edges,
            None => return false,
        };

        for (edge_map_kind, edge_key) in edges {
            match (self.options.filter_lossiness, edge_map_kind) {
                (None, _) => {}
                (Some(MapLossiness::Lossy), MapLossiness::Lossy) => {}
                (Some(MapLossiness::Perfect), MapLossiness::Perfect) => {}
                _ => return false,
            }

            self.path.push((def_id, edge_key.def_id));
            if self.probe_rec(edge_key.def_id) {
                return true;
            } else {
                self.path.pop();
            }
        }

        false
    }
}
