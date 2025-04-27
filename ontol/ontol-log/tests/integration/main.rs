use ontol_log::{
    log_model::EKind,
    logfile::{de::LogfileDeserializer, parser::parse_logfile, ser::to_string},
};
use serde::Deserialize;
use tracing::info;

mod test_diff;
mod test_examples;

fn verify_log(log: &[EKind]) {
    let logfile = to_string(log).unwrap();

    info!("log: {}", to_string(log).unwrap());

    let log2 =
        Vec::<EKind>::deserialize(LogfileDeserializer::new(parse_logfile(&logfile).unwrap()))
            .unwrap();

    assert_eq!(log, log2);
}
