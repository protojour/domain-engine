use fnv::FnvHashMap;
use ontol_runtime::{smart_format, DefId, RelationshipId};
use serde::ser::{SerializeMap, SerializeSeq};
use smartstring::alias::String;

use crate::{
    def::{DefKind, LookupRelationshipMeta},
    package::ONTOL_PKG,
    Compiler,
};

pub struct OntologyGraph<'cmp, 'm> {
    state: State<'m>,
    compiler: &'cmp Compiler<'m>,
}

impl<'cmp, 'm> From<&'cmp Compiler<'m>> for OntologyGraph<'cmp, 'm> {
    fn from(compiler: &'cmp Compiler<'m>) -> Self {
        Self {
            state: State::new(compiler),
            compiler,
        }
    }
}

struct State<'m> {
    node_meta_table: FnvHashMap<DefId, NodeMeta<'m>>,
    edge_meta_vec: Vec<EdgeMeta>,
}

struct NodeMeta<'m> {
    /// The node id in the graph structure
    node_id: String,
    /// The ONTOL identifier of the type
    ident: Option<&'m str>,
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

impl<'m> State<'m> {
    fn new(compiler: &Compiler<'m>) -> Self {
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
                        ident: Some(type_name),
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
                let subject = relationship.subject.0;
                let object = relationship.object.0;

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

        for key in compiler.codegen_tasks.result_map_proc_table.keys() {
            let input = key.input.def_id;
            let output = key.output.def_id;

            if let Some(meta) = node_meta_table.get_mut(&input) {
                meta.ref_count += 1;
            }
            if let Some(meta) = node_meta_table.get_mut(&output) {
                meta.ref_count += 1;
            }

            edge_meta_vec.push(EdgeMeta {
                source: input,
                target: output,
                kind: EdgeKind::Map,
            })
        }

        Self {
            node_meta_table,
            edge_meta_vec,
        }
    }
}

struct Nodes<'graph, 'm>(&'graph State<'m>, &'graph Compiler<'m>);
struct Edges<'graph, 'm>(&'graph State<'m>, &'graph Compiler<'m>);
struct Node<'g, 'm> {
    def_id: DefId,
    node_meta: &'g NodeMeta<'m>,
}
struct Edge<'graph, 'm> {
    source_meta: &'graph NodeMeta<'m>,
    target_meta: &'graph NodeMeta<'m>,
    relation_def_kind: Option<&'graph DefKind<'m>>,
}

impl<'graph, 'm> serde::Serialize for OntologyGraph<'graph, 'm> {
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

impl<'graph, 'm> serde::Serialize for Nodes<'graph, 'm> {
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

impl<'graph, 'm> serde::Serialize for Node<'graph, 'm> {
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

impl<'graph, 'm> serde::Serialize for Edges<'graph, 'm> {
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

impl<'graph, 'm> serde::Serialize for Edge<'graph, 'm> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("source", &self.source_meta.node_id)?;
        map.serialize_entry("target", &self.target_meta.node_id)?;

        match self.relation_def_kind {
            Some(DefKind::TextLiteral(literal)) => {
                map.serialize_entry("kind", "prop")?;
                map.serialize_entry("name", literal)?;
            }
            Some(DefKind::FmtTransition(..)) => {
                map.serialize_entry("kind", "fmt")?;
            }
            Some(DefKind::BuiltinRelType(_, ident)) => {
                if let Some(ident) = ident {
                    map.serialize_entry("kind", ident)?;
                }
            }
            Some(_) => unreachable!(),
            None => {
                // A mapping
                map.serialize_entry("kind", "map")?;
            }
        };

        map.end()
    }
}
