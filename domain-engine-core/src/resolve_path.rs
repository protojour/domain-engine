use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{env::Env, DefId, MapKey, PackageId};
use tracing::trace;

#[derive(Debug)]
pub struct ResolvePath {
    pub(crate) path: Vec<DefId>,
}

#[derive(Clone, Copy)]
pub struct ProbeOptions {
    pub must_be_entity: bool,
    pub inverted: bool,
}

pub struct ResolverGraph {
    // Graph of what each DefId can map to
    map_graph: FnvHashMap<MapKey, Vec<MapKey>>,
    // Graph of what each DefId can map from
    inverted_map_graph: FnvHashMap<MapKey, Vec<MapKey>>,
}

impl ResolverGraph {
    pub fn new(env: &Env) -> Self {
        let mut map_graph: FnvHashMap<MapKey, Vec<MapKey>> = Default::default();
        let mut inverted_map_graph: FnvHashMap<MapKey, Vec<MapKey>> = Default::default();

        for ((source_key, target_key), _) in env.mapper_procs() {
            map_graph.entry(source_key).or_default().push(target_key);
            inverted_map_graph
                .entry(target_key)
                .or_default()
                .push(source_key);
        }

        Self {
            map_graph,
            inverted_map_graph,
        }
    }

    pub fn probe_path(
        &self,
        env: &Env,
        public_def_id: DefId,
        target_package: PackageId,
        options: ProbeOptions,
    ) -> Option<ResolvePath> {
        let mut path = vec![];
        let mut visited = Default::default();
        let mut probe = Probe {
            env,
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

struct Probe<'e, 'a> {
    env: &'e Env,
    graph: &'e FnvHashMap<MapKey, Vec<MapKey>>,
    options: &'e ProbeOptions,
    path: &'a mut Vec<DefId>,
    visited: &'a mut FnvHashSet<DefId>,
    target_package: PackageId,
}

impl<'e, 'a> Probe<'e, 'a> {
    fn probe_rec(&mut self, def_id: DefId) -> bool {
        trace!("probe {def_id:?} path={:?}", self.path);

        if !self.visited.insert(def_id) {
            return false;
        }

        if self.options.must_be_entity {
            let type_info = self.env.get_type_info(def_id);
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

        for edge in edges {
            self.path.push(edge.def_id);
            if self.probe_rec(edge.def_id) {
                return true;
            } else {
                self.path.pop();
            }
        }

        false
    }
}
