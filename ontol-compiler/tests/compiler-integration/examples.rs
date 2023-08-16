use ontol_test_utils::SourceName;

type Example = (SourceName, &'static str);

pub const GEOJSON: Example = (
    SourceName("geojson"),
    include_str!("../../../examples/geojson.on"),
);

pub const STIX: Example = (
    SourceName("stix"),
    include_str!("../../../examples/stix_lite/stix.on"),
);

pub const STIX_COMMON: Example = (
    SourceName("stix_common"),
    include_str!("../../../examples/stix_lite/stix_common.on"),
);

pub const STIX_META: Example = (
    SourceName("stix_meta"),
    include_str!("../../../examples/stix_lite/stix_meta.on"),
);

pub const STIX_OPEN_VOCAB: Example = (
    SourceName("stix_open_vocab"),
    include_str!("../../../examples/stix_lite/stix_open_vocab.on"),
);

pub const SI: Example = (SourceName("SI"), include_str!("../../../examples/SI.on"));

pub const WGS: Example = (SourceName("wgs"), include_str!("../../../examples/wgs.on"));
