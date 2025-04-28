use std::sync::Arc;

use conduit::{blog_post_public, conduit_db};
use ontol_core::url::{DomainUrl, DomainUrlResolver};
use stix::stix_bundle;
use url::Url;

type Example = (DomainUrl, Arc<String>);

fn file_url(name: &str) -> DomainUrl {
    DomainUrl::parse(name)
}

macro_rules! include_arcstring {
    ($path:literal) => {
        std::sync::Arc::new(include_str!($path).to_string())
    };
}

pub mod stix {
    use super::*;

    pub fn stix() -> Example {
        (
            file_url("stix"),
            include_arcstring!("../../../examples/stix_lite/stix.on"),
        )
    }

    pub fn stix_edges() -> Example {
        (
            file_url("stix_edges"),
            include_arcstring!("../../../examples/stix_lite/stix_edges.on"),
        )
    }

    pub fn stix_common() -> Example {
        (
            file_url("stix_common"),
            include_arcstring!("../../../examples/stix_lite/stix_common.on"),
        )
    }

    pub fn stix_interface() -> Example {
        (
            file_url("stix_interface"),
            include_arcstring!("../../../examples/stix_lite/stix_interface.on"),
        )
    }

    pub fn stix_open_vocab() -> Example {
        (
            file_url("stix_open_vocab"),
            include_arcstring!("../../../examples/stix_lite/stix_open_vocab.on"),
        )
    }

    pub fn stix_bundle() -> Vec<Example> {
        vec![
            stix(),
            stix_edges(),
            stix_interface(),
            stix_common(),
            stix_open_vocab(),
            super::si().as_atlas("SI"),
        ]
    }
}

pub mod conduit {
    use super::*;

    pub fn conduit_db() -> Example {
        (
            file_url("conduit_db"),
            include_arcstring!("../../../examples/conduit/conduit_db.on"),
        )
    }

    pub fn conduit_public() -> Example {
        (
            file_url("conduit_public"),
            include_arcstring!("../../../examples/conduit/TODO_conduit_public.on"),
        )
    }

    pub fn conduit_contrived_signup() -> Example {
        (
            file_url("conduit_contrived_signup"),
            include_arcstring!("../../../examples/conduit/TODO_conduit_contrived_signup.on"),
        )
    }

    pub fn blog_post_public() -> Example {
        (
            file_url("blog_post_public"),
            include_arcstring!("../../../examples/conduit/blog_post_public.on"),
        )
    }

    pub fn feed_public() -> Example {
        (
            file_url("feed_public"),
            include_arcstring!("../../../examples/conduit/feed_public.on"),
        )
    }
}

pub fn artist_and_instrument() -> Example {
    (
        file_url("artist_and_instrument"),
        include_arcstring!("../../../examples/artist_and_instrument.on"),
    )
}

pub fn demo() -> Example {
    (
        file_url("demo"),
        include_arcstring!("../../../examples/demo.on"),
    )
}

pub fn filemeta() -> Example {
    (
        file_url("filemeta"),
        include_arcstring!("../../../examples/filemeta.on"),
    )
}

pub fn findings() -> Example {
    (
        file_url("findings"),
        include_arcstring!("../../../examples/findings.on"),
    )
}

pub fn edge_entity_simple() -> Example {
    (
        file_url("edge_entity"),
        include_arcstring!("../../../examples/edge_entity_simple.on"),
    )
}

pub fn edge_entity_union() -> Example {
    (
        file_url("edge_entity_union"),
        include_arcstring!("../../../examples/edge_entity_union.on"),
    )
}

pub fn gitmesh() -> Example {
    (
        file_url("gitmesh"),
        include_arcstring!("../../../examples/gitmesh.on"),
    )
}

pub fn geojson() -> Example {
    (
        file_url("geojson"),
        include_arcstring!("../../../examples/geojson.on"),
    )
}

pub fn guitar_synth_union() -> Example {
    (
        file_url("guitar_synth_union"),
        include_arcstring!("../../../examples/guitar_synth_union.on"),
    )
}

pub fn municipalities() -> Example {
    (
        file_url("municipalities"),
        include_arcstring!("../../../examples/municipalities.on"),
    )
}

pub fn si() -> Example {
    (
        file_url("SI"),
        include_arcstring!("../../../examples/SI.on"),
    )
}

pub fn workspaces() -> Example {
    (
        file_url("workspaces"),
        include_arcstring!("../../../examples/workspaces.on"),
    )
}

pub fn wgs() -> Example {
    (
        file_url("wgs"),
        include_arcstring!("../../../examples/wgs.on"),
    )
}

pub mod entity_subtype {
    use super::*;

    pub fn db() -> Example {
        (
            file_url("db"),
            include_arcstring!("../../../examples/entity_subtype/db.on"),
        )
    }

    pub fn derived() -> Example {
        (
            file_url("derived"),
            include_arcstring!("../../../examples/entity_subtype/derived.on"),
        )
    }
}

pub trait AsAtlas {
    fn as_atlas(&self, bundle_name: &str) -> Self;
}

impl AsAtlas for DomainUrl {
    fn as_atlas(&self, bundle_name: &str) -> Self {
        if self.url().scheme() == "atlas" {
            self.clone()
        } else {
            let mut path_segments = self.url().path_segments().unwrap();
            let file_name = path_segments.next_back().unwrap();

            DomainUrl::new(
                Url::parse(&format!("atlas:/protojour/{bundle_name}/{file_name}")).unwrap(),
            )
        }
    }
}

impl AsAtlas for Example {
    fn as_atlas(&self, bundle_name: &str) -> Self {
        (self.0.as_atlas(bundle_name), self.1.clone())
    }
}

impl AsAtlas for Vec<Example> {
    fn as_atlas(&self, bundle_name: &str) -> Self {
        self.iter()
            .map(|example| example.as_atlas(bundle_name))
            .collect()
    }
}

/// A "resolver" that can resolve Atlas URLs to example files
pub struct FakeAtlasServer {
    domains: Vec<AtlasDomain>,
}

pub enum Visibility {
    /// The domain is an entrypoint in a package
    Entrypoint,
    /// The domain is private within a package
    Private,
}

pub struct AtlasDomain {
    pub url: DomainUrl,
    /// Visibility within a package of domains that shares a common URL prefix
    pub visibility: Visibility,
    pub src: Arc<String>,
}

#[async_trait::async_trait]
impl DomainUrlResolver for FakeAtlasServer {
    async fn resolve_domain_url(&self, url: &DomainUrl) -> Option<Arc<String>> {
        self.lookup(url)
    }
}

impl FakeAtlasServer {
    pub fn lookup(&self, url: &DomainUrl) -> Option<Arc<String>> {
        self.domains.iter().find_map(|dom| {
            if &dom.url == url {
                Some(dom.src.clone())
            } else {
                None
            }
        })
    }

    pub fn list(&self) -> impl Iterator<Item = &AtlasDomain> + use<'_> {
        self.domains.iter()
    }
}

impl Default for FakeAtlasServer {
    fn default() -> Self {
        let mut domains = vec![];

        add_atlas("geojson", geojson(), Visibility::Entrypoint, &mut domains);
        add_atlas("wgs", wgs(), Visibility::Entrypoint, &mut domains);
        add_atlas("SI", si(), Visibility::Entrypoint, &mut domains);
        add_atlas("filemeta", filemeta(), Visibility::Entrypoint, &mut domains);
        add_atlas("findings", findings(), Visibility::Entrypoint, &mut domains);

        for (index, example) in stix_bundle().iter().cloned().enumerate() {
            let visibility = if index == 0 {
                Visibility::Entrypoint
            } else {
                Visibility::Private
            };
            add_atlas("stix", example, visibility, &mut domains);
        }

        add_atlas(
            "conduit",
            conduit_db(),
            Visibility::Entrypoint,
            &mut domains,
        );
        add_atlas(
            "conduit",
            blog_post_public(),
            Visibility::Entrypoint,
            &mut domains,
        );
        add_atlas("gitmesh", gitmesh(), Visibility::Entrypoint, &mut domains);
        add_atlas(
            "workspaces",
            workspaces(),
            Visibility::Entrypoint,
            &mut domains,
        );

        Self { domains }
    }
}

fn add_atlas(
    bundle_name: &str,
    example: Example,
    visibility: Visibility,
    domains: &mut Vec<AtlasDomain>,
) {
    let (atlas_url, src) = example.as_atlas(bundle_name);
    if !domains.iter().any(|dom| dom.url == atlas_url) {
        domains.push(AtlasDomain {
            url: atlas_url.clone(),
            visibility: Visibility::Private,
            src,
        });
    }

    if matches!(visibility, Visibility::Entrypoint) {
        domains
            .iter_mut()
            .find(|dom| dom.url == atlas_url)
            .unwrap()
            .visibility = Visibility::Entrypoint;
    }
}
