use pest::{Parser, iterators::Pair};
use pest_derive::Parser;

/// The ONTOL logfile language parser
#[derive(Parser)]
#[grammar = "../grammar/logfile.pest"]
pub(super) struct LogfileParser;

pub fn parse_logfile(input: &str) -> Result<Pair<'_, Rule>, pest::error::Error<Rule>> {
    Ok(LogfileParser::parse(Rule::logfile, input)?.next().unwrap())
}
