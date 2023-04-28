use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use test_log::test;

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.on");

const WORK: &str = "
pub type worker_id { fmt '' => 'worker/' => uuid => _ }
pub type tech_id { fmt '' => 'tech/' => uuid => _ }

pub type worker
pub type technology

with worker {
    rel _ 'ID': worker_id
    rel worker_id identifies: _
    rel _ 'name': string
    
    rel _ 'technologies': [technology]
}

with technology {
    rel _ 'ID': tech_id
    rel tech_id identifies: _
    rel _ 'name': string
}
";

const DEV: &str = "
pub type lang_id { fmt '' => uuid => _ }
pub type dev_id { fmt '' => uuid => _ }

pub type language
pub type developer

with language {
    rel _ 'id': lang_id
    rel lang_id identifies: _

    rel _ 'name': string
    rel _ 'developers': [developer]
}

with developer {
    rel _ 'id': dev_id
    rel dev_id identifies: _
    rel _ 'name': string
}
";

#[test]
fn test_map_invert() {
    TestPackages::with_sources([
        (SourceName("work"), WORK),
        (SourceName("dev"), DEV),
        (
            SourceName::root(),
            "
            use 'work' as work
            use 'dev' as dev

            map p_id p_name tech_id tech_name {
                work.worker {
                    'ID': p_id
                    'name': p_name
                    'technologies': [work.technology {
                        'ID': tech_id
                        'name': tech_name
                    }]
                }
                dev.language {
                    'id': tech_id
                    'name': tech_name
                    'developers': [dev.developer { // ERROR TODO: Incompatible aggregation group
                        'id': p_id
                        'name': p_name
                    }]
                }
            }
            ",
        ),
    ])
    .compile_fail()
}

#[test]
fn artist_etc_routing() {
    TestPackages::with_sources([
        (SourceName("gsu"), GUITAR_SYNTH_UNION),
        (SourceName("ai"), ARTIST_AND_INSTRUMENT),
        (
            SourceName::root(),
            "
            use 'gsu' as gsu
            use 'ai' as ai

            rel gsu route: ai {
                map id n p {
                    gsu.artist {
                        'artist-id': id // ERROR cannot convert this `artist_id` from `artist-id`: These types are not equated.
                        'name': n
                        'plays': [p] // ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                    ai.artist {
                        'ID': id // ERROR cannot convert this `artist-id` from `artist_id`: These types are not equated.
                        'name': n
                        'plays': [p] // ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                }

                map id t p n {
                    gsu.synth {
                        'instrument-id': id
                        'type': t
                        'polyphony': p
                    }
                    ai.instrument {
                        'ID': id
                        'name': n
                    }
                }
            }
            ",
        ),
    ])
    .compile_fail()
}
