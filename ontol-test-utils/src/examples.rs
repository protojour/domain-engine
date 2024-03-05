use crate::SrcName;

type Example = (SrcName, &'static str);

pub mod stix {
    use crate::TestPackages;

    use super::*;

    pub const STIX: Example = (
        SrcName("stix"),
        include_str!("../../examples/stix_lite/stix.on"),
    );

    pub const STIX_COMMON: Example = (
        SrcName("stix_common"),
        include_str!("../../examples/stix_lite/stix_common.on"),
    );

    pub const STIX_META: Example = (
        SrcName("stix_meta"),
        include_str!("../../examples/stix_lite/stix_meta.on"),
    );

    pub const STIX_OPEN_VOCAB: Example = (
        SrcName("stix_open_vocab"),
        include_str!("../../examples/stix_lite/stix_open_vocab.on"),
    );

    pub fn stix_bundle() -> TestPackages {
        TestPackages::with_sources([STIX, STIX_META, STIX_COMMON, STIX_OPEN_VOCAB, SI])
    }
}

pub mod conduit {
    use super::*;

    pub const CONDUIT_DB: Example = (
        SrcName("conduit_db"),
        include_str!("../../examples/conduit/conduit_db.on"),
    );

    pub const CONDUIT_PUBLIC: Example = (
        SrcName("conduit_public"),
        include_str!("../../examples/conduit/conduit_public.on"),
    );

    pub const CONDUIT_CONTRIVED_SIGNUP: Example = (
        SrcName("conduit_contrived_signup"),
        include_str!("../../examples/conduit/conduit_contrived_signup.on"),
    );

    pub const BLOG_POST_PUBLIC: Example = (
        SrcName("blog_post_public"),
        include_str!("../../examples/conduit/blog_post_public.on"),
    );

    pub const FEED_PUBLIC: Example = (
        SrcName("feed_public"),
        include_str!("../../examples/conduit/feed_public.on"),
    );
}

pub const ARTIST_AND_INSTRUMENT: Example = (
    SrcName("artist_and_instrument"),
    include_str!("../../examples/artist_and_instrument.on"),
);

pub const DEMO: Example = (SrcName("demo"), include_str!("../../examples/demo.on"));

pub const GITMESH: Example = (
    SrcName("gitmesh"),
    include_str!("../../examples/gitmesh.on"),
);

pub const GEOJSON: Example = (
    SrcName("geojson"),
    include_str!("../../examples/geojson.on"),
);

pub const GUITAR_SYNTH_UNION: Example = (
    SrcName("guitar_synth_union"),
    include_str!("../../examples/guitar_synth_union.on"),
);

pub const MUNICIPALITIES: Example = (
    SrcName("municipalities"),
    include_str!("../../examples/municipalities.on"),
);

pub const SI: Example = (SrcName("SI"), include_str!("../../examples/SI.on"));

pub const WGS: Example = (SrcName("wgs"), include_str!("../../examples/wgs.on"));
