use crate::SourceName;

type Example = (SourceName, &'static str);

pub mod stix {
    use super::*;

    pub const STIX: Example = (
        SourceName("stix"),
        include_str!("../../examples/stix_lite/stix.on"),
    );

    pub const STIX_COMMON: Example = (
        SourceName("stix_common"),
        include_str!("../../examples/stix_lite/stix_common.on"),
    );

    pub const STIX_META: Example = (
        SourceName("stix_meta"),
        include_str!("../../examples/stix_lite/stix_meta.on"),
    );

    pub const STIX_OPEN_VOCAB: Example = (
        SourceName("stix_open_vocab"),
        include_str!("../../examples/stix_lite/stix_open_vocab.on"),
    );
}

pub mod conduit {
    use super::*;

    pub const CONDUIT_DB: Example = (
        SourceName("conduit_db"),
        include_str!("../../examples/conduit/conduit_db.on"),
    );

    pub const CONDUIT_PUBLIC: Example = (
        SourceName("conduit_public"),
        include_str!("../../examples/conduit/conduit_public.on"),
    );

    pub const CONDUIT_CONTRIVED_SIGNUP: Example = (
        SourceName("conduit_contrived_signup"),
        include_str!("../../examples/conduit/conduit_contrived_signup.on"),
    );

    pub const BLOG_POST_PUBLIC: Example = (
        SourceName("blog_post_public"),
        include_str!("../../examples/conduit/blog_post_public.on"),
    );
}

pub const ARTIST_AND_INSTRUMENT: Example = (
    SourceName("artist_and_instrument"),
    include_str!("../../examples/artist_and_instrument.on"),
);

pub const DEMO: Example = (SourceName("demo"), include_str!("../../examples/demo.on"));

pub const GEOJSON: Example = (
    SourceName("geojson"),
    include_str!("../../examples/geojson.on"),
);

pub const GUITAR_SYNTH_UNION: Example = (
    SourceName("guitar_synth_union"),
    include_str!("../../examples/guitar_synth_union.on"),
);

pub const MUNICIPALITIES: Example = (
    SourceName("municipalities"),
    include_str!("../../examples/municipalities.on"),
);

pub const SI: Example = (SourceName("SI"), include_str!("../../examples/SI.on"));

pub const WGS: Example = (SourceName("wgs"), include_str!("../../examples/wgs.on"));

pub trait Root {
    fn root(self) -> Self;
}

impl Root for (SourceName, &'static str) {
    fn root(self) -> Self {
        (SourceName::root(), self.1)
    }
}
