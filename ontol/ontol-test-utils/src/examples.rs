use crate::{src_name, SrcName};

type Example = (SrcName, &'static str);

pub mod stix {
    use crate::TestPackages;

    use super::*;

    pub const STIX: Example = (
        src_name("stix"),
        include_str!("../../../examples/stix_lite/stix.on"),
    );

    pub const STIX_EDGES: Example = (
        src_name("stix_edges"),
        include_str!("../../../examples/stix_lite/stix_edges.on"),
    );

    pub const STIX_COMMON: Example = (
        src_name("stix_common"),
        include_str!("../../../examples/stix_lite/stix_common.on"),
    );

    pub const STIX_INTERFACE: Example = (
        src_name("stix_interface"),
        include_str!("../../../examples/stix_lite/stix_interface.on"),
    );

    pub const STIX_OPEN_VOCAB: Example = (
        src_name("stix_open_vocab"),
        include_str!("../../../examples/stix_lite/stix_open_vocab.on"),
    );

    pub fn stix_bundle() -> TestPackages {
        TestPackages::with_static_sources([
            STIX,
            STIX_EDGES,
            STIX_INTERFACE,
            STIX_COMMON,
            STIX_OPEN_VOCAB,
            SI,
        ])
    }
}

pub mod conduit {
    use super::*;

    pub const CONDUIT_DB: Example = (
        src_name("conduit_db"),
        include_str!("../../../examples/conduit/conduit_db.on"),
    );

    pub const CONDUIT_PUBLIC: Example = (
        src_name("conduit_public"),
        include_str!("../../../examples/conduit/TODO_conduit_public.on"),
    );

    pub const CONDUIT_CONTRIVED_SIGNUP: Example = (
        src_name("conduit_contrived_signup"),
        include_str!("../../../examples/conduit/TODO_conduit_contrived_signup.on"),
    );

    pub const BLOG_POST_PUBLIC: Example = (
        src_name("blog_post_public"),
        include_str!("../../../examples/conduit/blog_post_public.on"),
    );

    pub const FEED_PUBLIC: Example = (
        src_name("feed_public"),
        include_str!("../../../examples/conduit/feed_public.on"),
    );
}

pub const ARTIST_AND_INSTRUMENT: Example = (
    src_name("artist_and_instrument"),
    include_str!("../../../examples/artist_and_instrument.on"),
);

pub const DEMO: Example = (src_name("demo"), include_str!("../../../examples/demo.on"));

pub const FINDINGS: Example = (
    src_name("findings"),
    include_str!("../../../examples/findings.on"),
);

pub const EDGE_ENTITY_SIMPLE: Example = (
    src_name("edge_entity"),
    include_str!("../../../examples/edge_entity_simple.on"),
);

pub const EDGE_ENTITY_UNION: Example = (
    src_name("edge_entity_union"),
    include_str!("../../../examples/edge_entity_union.on"),
);

pub const GITMESH: Example = (
    src_name("gitmesh"),
    include_str!("../../../examples/gitmesh.on"),
);

pub const GEOJSON: Example = (
    src_name("geojson"),
    include_str!("../../../examples/geojson.on"),
);

pub const GUITAR_SYNTH_UNION: Example = (
    src_name("guitar_synth_union"),
    include_str!("../../../examples/guitar_synth_union.on"),
);

pub const MUNICIPALITIES: Example = (
    src_name("municipalities"),
    include_str!("../../../examples/municipalities.on"),
);

pub const SI: Example = (src_name("SI"), include_str!("../../../examples/SI.on"));

pub const WGS: Example = (src_name("wgs"), include_str!("../../../examples/wgs.on"));

pub mod entity_subtype {
    use super::*;

    pub const DB: Example = (
        src_name("db"),
        include_str!("../../../examples/entity_subtype/db.on"),
    );

    pub const DERIVED: Example = (
        src_name("derived"),
        include_str!("../../../examples/entity_subtype/derived.on"),
    );
}
