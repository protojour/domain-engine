use fnv::FnvHashMap;
use ontol_runtime::{smart_format, DefId, RelationshipId};
use serde::ser::{SerializeMap, SerializeSeq};
use smartstring::alias::String;

use crate::{
    def::{BuiltinRelationKind, DefKind, LookupRelationshipMeta},
    package::ONTOL_PKG,
    Compiler,
};

pub struct OntologyGraph<'c, 'm> {
    state: State,
    compiler: &'c Compiler<'m>,
}

impl<'c, 'm> From<&'c Compiler<'m>> for OntologyGraph<'c, 'm> {
    fn from(compiler: &'c Compiler<'m>) -> Self {
        Self {
            state: State::new(compiler),
            compiler,
        }
    }
}

struct State {
    node_meta_table: FnvHashMap<DefId, NodeMeta>,
    edge_meta_vec: Vec<EdgeMeta>,
}

struct NodeMeta {
    /// The node id in the graph structure
    node_id: String,
    /// The ONTOL identifier of the type
    ident: Option<String>,
    ref_count: usize,
}

struct EdgeMeta {
    source: DefId,
    target: DefId,
    kind: EdgeKind,
}

enum EdgeKind {
    Relationship(RelationshipId),
    Map,
}

impl State {
    fn new(compiler: &Compiler) -> Self {
        let mut node_meta_table = FnvHashMap::default();
        let mut edge_meta_vec = vec![];

        let namespaces = &compiler.namespaces.namespaces;

        for package_id in compiler.package_ids() {
            let Some(namespace) = namespaces.get(&package_id) else {
                continue;
            };

            for (type_name, type_def_id) in &namespace.types {
                node_meta_table.insert(
                    *type_def_id,
                    NodeMeta {
                        node_id: smart_format!("{}_{type_name}", package_id.0),
                        ident: Some(type_name.clone()),
                        ref_count: 0,
                    },
                );
            }

            for type_def_id in &namespace.anonymous {
                node_meta_table.insert(
                    *type_def_id,
                    NodeMeta {
                        node_id: smart_format!("{}___anon{}", package_id.0, type_def_id.1),
                        ident: None,
                        ref_count: 0,
                    },
                );
            }
        }

        for (def_id, def) in &compiler.defs.table {
            if let DefKind::Relationship(relationship) = &def.kind {
                let subject = relationship.subject.0.def_id;
                let object = relationship.object.0.def_id;

                if let Some(meta) = node_meta_table.get_mut(&subject) {
                    meta.ref_count += 1;
                }
                if let Some(meta) = node_meta_table.get_mut(&object) {
                    meta.ref_count += 1;
                }

                edge_meta_vec.push(EdgeMeta {
                    source: subject,
                    target: object,
                    kind: EdgeKind::Relationship(RelationshipId(*def_id)),
                });
            }
        }

        for (source_key, target_key) in compiler.codegen_tasks.result_map_proc_table.keys() {
            let source = source_key.def_id;
            let target = target_key.def_id;

            if let Some(meta) = node_meta_table.get_mut(&source) {
                meta.ref_count += 1;
            }
            if let Some(meta) = node_meta_table.get_mut(&target) {
                meta.ref_count += 1;
            }

            edge_meta_vec.push(EdgeMeta {
                source,
                target,
                kind: EdgeKind::Map,
            })
        }

        Self {
            node_meta_table,
            edge_meta_vec,
        }
    }
}

struct Nodes<'g, 'm>(&'g State, &'g Compiler<'m>);
struct Edges<'g, 'm>(&'g State, &'g Compiler<'m>);
struct Node<'g> {
    def_id: DefId,
    node_meta: &'g NodeMeta,
}
struct Edge<'g, 'm> {
    source_meta: &'g NodeMeta,
    target_meta: &'g NodeMeta,
    relation_def_kind: Option<&'g DefKind<'m>>,
}

impl<'g, 'm> serde::Serialize for OntologyGraph<'g, 'm> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("nodes", &Nodes(&self.state, self.compiler))?;
        map.serialize_entry("links", &Edges(&self.state, self.compiler))?;
        map.end()
    }
}

impl<'g, 'm> serde::Serialize for Nodes<'g, 'm> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let state = self.0;

        let mut list = serializer.serialize_seq(None)?;

        for (def_id, node_meta) in &state.node_meta_table {
            if def_id.package_id() == ONTOL_PKG && node_meta.ref_count == 0 {
                continue;
            }

            list.serialize_element(&Node {
                def_id: *def_id,
                node_meta,
            })?;
        }

        list.end()
    }
}

impl<'g> serde::Serialize for Node<'g> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.node_meta.node_id)?;
        map.serialize_entry("package_id", &self.def_id.0 .0)?;
        map.serialize_entry("ident", &self.node_meta.ident)?;

        map.end()
    }
}

impl<'g, 'm> serde::Serialize for Edges<'g, 'm> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let state = self.0;
        let compiler = self.1;
        let mut list = serializer.serialize_seq(None)?;

        for edge_meta in &state.edge_meta_vec {
            let source_meta = state.node_meta_table.get(&edge_meta.source);
            let target_meta = state.node_meta_table.get(&edge_meta.target);

            if let (Some(source_meta), Some(target_meta)) = (source_meta, target_meta) {
                match &edge_meta.kind {
                    EdgeKind::Relationship(relationship_id) => {
                        let meta = compiler.defs.relationship_meta(*relationship_id);
                        list.serialize_element(&Edge {
                            source_meta,
                            target_meta,
                            relation_def_kind: Some(&meta.relation_def_kind),
                        })?
                    }
                    EdgeKind::Map => {
                        list.serialize_element(&Edge {
                            source_meta,
                            target_meta,
                            relation_def_kind: None,
                        })?;
                    }
                }
            }
        }

        list.end()
    }
}

impl<'g, 'm> serde::Serialize for Edge<'g, 'm> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("source", &self.source_meta.node_id)?;
        map.serialize_entry("target", &self.target_meta.node_id)?;

        match self.relation_def_kind {
            Some(DefKind::StringLiteral(literal)) => {
                map.serialize_entry("kind", "prop")?;
                map.serialize_entry("name", literal)?;
            }
            Some(DefKind::FmtTransition(..)) => {
                map.serialize_entry("kind", "fmt")?;
            }
            Some(DefKind::BuiltinRelType(kind)) => match kind {
                BuiltinRelationKind::Is => {
                    map.serialize_entry("kind", "is")?;
                }
                BuiltinRelationKind::Identifies => {
                    map.serialize_entry("kind", "identifies")?;
                }
                BuiltinRelationKind::Id => {
                    map.serialize_entry("kind", "id")?;
                }
                BuiltinRelationKind::Indexed => {
                    map.serialize_entry("kind", "indexed")?;
                }
                BuiltinRelationKind::Min => {
                    map.serialize_entry("kind", "min")?;
                }
                BuiltinRelationKind::Max => {
                    map.serialize_entry("kind", "max")?;
                }
                BuiltinRelationKind::Default => {
                    map.serialize_entry("kind", "default")?;
                }
                BuiltinRelationKind::Gen => {
                    map.serialize_entry("kind", "gen")?;
                }
                BuiltinRelationKind::Route => {
                    map.serialize_entry("kind", "route")?;
                }
                BuiltinRelationKind::Doc => {
                    map.serialize_entry("kind", "doc")?;
                }
                BuiltinRelationKind::Example => {
                    map.serialize_entry("kind", "example")?;
                }
            },
            Some(_) => unreachable!(),
            None => {
                // A mapping
                map.serialize_entry("kind", "map")?;
            }
        };

        map.end()
    }
}
