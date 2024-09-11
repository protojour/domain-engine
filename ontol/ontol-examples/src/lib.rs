use conduit::{blog_post_public, conduit_db};
use ontol_compiler::topology::{DomainReferenceParser, DomainUrl};
use stix::stix_bundle;
use url::Url;

type Example = (DomainUrl, &'static str);

fn file_url(name: &str) -> DomainUrl {
    DomainReferenceParser::default()
        .parse(name)
        .unwrap_or_else(|_| panic!("invalid name"))
        .as_url()
}

pub mod stix {
    use super::*;

    pub fn stix() -> Example {
        (
            file_url("stix"),
            include_str!("../../../examples/stix_lite/stix.on"),
        )
    }

    pub fn stix_edges() -> Example {
        (
            file_url("stix_edges"),
            include_str!("../../../examples/stix_lite/stix_edges.on"),
        )
    }

    pub fn stix_common() -> Example {
        (
            file_url("stix_common"),
            include_str!("../../../examples/stix_lite/stix_common.on"),
        )
    }

    pub fn stix_interface() -> Example {
        (
            file_url("stix_interface"),
            include_str!("../../../examples/stix_lite/stix_interface.on"),
        )
    }

    pub fn stix_open_vocab() -> Example {
        (
            file_url("stix_open_vocab"),
            include_str!("../../../examples/stix_lite/stix_open_vocab.on"),
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
            include_str!("../../../examples/conduit/conduit_db.on"),
        )
    }

    pub fn conduit_public() -> Example {
        (
            file_url("conduit_public"),
            include_str!("../../../examples/conduit/TODO_conduit_public.on"),
        )
    }

    pub fn conduit_contrived_signup() -> Example {
        (
            file_url("conduit_contrived_signup"),
            include_str!("../../../examples/conduit/TODO_conduit_contrived_signup.on"),
        )
    }

    pub fn blog_post_public() -> Example {
        (
            file_url("blog_post_public"),
            include_str!("../../../examples/conduit/blog_post_public.on"),
        )
    }

    pub fn feed_public() -> Example {
        (
            file_url("feed_public"),
            include_str!("../../../examples/conduit/feed_public.on"),
        )
    }
}

pub fn artist_and_instrument() -> Example {
    (
        file_url("artist_and_instrument"),
        include_str!("../../../examples/artist_and_instrument.on"),
    )
}

pub fn demo() -> Example {
    (file_url("demo"), include_str!("../../../examples/demo.on"))
}

pub fn findings() -> Example {
    (
        file_url("findings"),
        include_str!("../../../examples/findings.on"),
    )
}

pub fn edge_entity_simple() -> Example {
    (
        file_url("edge_entity"),
        include_str!("../../../examples/edge_entity_simple.on"),
    )
}

pub fn edge_entity_union() -> Example {
    (
        file_url("edge_entity_union"),
        include_str!("../../../examples/edge_entity_union.on"),
    )
}

pub fn gitmesh() -> Example {
    (
        file_url("gitmesh"),
        include_str!("../../../examples/gitmesh.on"),
    )
}

pub fn geojson() -> Example {
    (
        file_url("geojson"),
        include_str!("../../../examples/geojson.on"),
    )
}

pub fn guitar_synth_union() -> Example {
    (
        file_url("guitar_synth_union"),
        include_str!("../../../examples/guitar_synth_union.on"),
    )
}

pub fn municipalities() -> Example {
    (
        file_url("municipalities"),
        include_str!("../../../examples/municipalities.on"),
    )
}

pub fn si() -> Example {
    (file_url("SI"), include_str!("../../../examples/SI.on"))
}

pub fn wgs() -> Example {
    (file_url("wgs"), include_str!("../../../examples/wgs.on"))
}

pub mod entity_subtype {
    use super::*;

    pub fn db() -> Example {
        (
            file_url("db"),
            include_str!("../../../examples/entity_subtype/db.on"),
        )
    }

    pub fn derived() -> Example {
        (
            file_url("derived"),
            include_str!("../../../examples/entity_subtype/derived.on"),
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
            let path_segments = self.url().path_segments().unwrap();
            let file_name = path_segments.last().unwrap();

            DomainUrl::new(
                Url::parse(&format!("atlas:/protojour/{bundle_name}/{file_name}")).unwrap(),
            )
        }
    }
}

impl AsAtlas for Example {
    fn as_atlas(&self, bundle_name: &str) -> Self {
        (self.0.as_atlas(bundle_name), self.1)
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
    sources: Vec<(DomainUrl, &'static str)>,
}

impl FakeAtlasServer {
    pub fn lookup(&self, url: &DomainUrl) -> Option<&'static str> {
        self.sources.iter().find_map(
            |(atlas_url, src)| {
                if atlas_url == url {
                    Some(*src)
                } else {
                    None
                }
            },
        )
    }
}

impl Default for FakeAtlasServer {
    fn default() -> Self {
        let mut sources = vec![];

        add_atlas("geojson", geojson(), &mut sources);
        add_atlas("wgs", wgs(), &mut sources);
        add_atlas("SI", si(), &mut sources);

        for example in stix_bundle() {
            add_atlas("stix", example, &mut sources);
        }

        add_atlas("conduit", conduit_db(), &mut sources);
        add_atlas("conduit", blog_post_public(), &mut sources);
        add_atlas("gitmesh", gitmesh(), &mut sources);
        add_atlas("findings", findings(), &mut sources);

        Self { sources }
    }
}

fn add_atlas(bundle_name: &str, example: Example, sources: &mut Vec<(DomainUrl, &'static str)>) {
    let (atlas_url, src) = example.as_atlas(bundle_name);
    if !sources.iter().any(|(url, _)| url == &atlas_url) {
        sources.push((atlas_url, src));
    }
}
