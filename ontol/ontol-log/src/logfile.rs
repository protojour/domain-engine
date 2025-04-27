pub mod de;
pub mod parser;
pub mod ser;

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::{
        log_model::{EKind, TypeRef},
        tag::Tag,
        token::Token,
    };

    use ontol_parser::lexer::kind::Kind;
    use ontol_syntax::rowan::{GreenToken, SyntaxKind};
    use serde::{Deserialize, Serialize, de::DeserializeOwned};

    use super::{de::LogfileDeserializer, parser::parse_logfile, ser::to_string};

    fn test_serde<T: Sized + Debug + Serialize + DeserializeOwned + PartialEq>(value: Vec<T>) {
        let logfile = to_string(&value).unwrap();
        println!("wrote: {logfile}");
        let value2 =
            Vec::<T>::deserialize(LogfileDeserializer::new(parse_logfile(&logfile).unwrap()))
                .unwrap();

        assert_eq!(value, value2);
    }

    #[test]
    fn test_parse_logfile() {
        let input = "
            ; comment
            (start \"01ARZ3NDEKTSV4RRFFQ69G5FAV\")
            (sub-add #42 ())
        ";

        let pair = parse_logfile(input).unwrap();
        let des = LogfileDeserializer::new(pair);

        let b = Vec::<EKind>::deserialize(des).unwrap();

        assert_eq!(b.len(), 2);
    }

    #[test]
    fn test_token() {
        test_serde(vec![Token(GreenToken::new(
            SyntaxKind(Kind::Whitespace as u16),
            "foo",
        ))]);
    }

    #[test]
    fn test_tag() {
        test_serde(vec![Tag(42)]);
    }

    #[test]
    fn test_parse_bool() {
        let des = LogfileDeserializer::new(parse_logfile("false").unwrap());

        assert_eq!(Vec::<bool>::deserialize(des).unwrap(), vec![false]);
    }

    #[test]
    fn test_parse_range() {
        let des = LogfileDeserializer::new(parse_logfile("(number-range \"0\" \"1\")").unwrap());

        assert_eq!(
            Vec::<TypeRef>::deserialize(des).unwrap(),
            vec![TypeRef::NumberRange(Some("0".into()), Some("1".into()))]
        );

        let des = LogfileDeserializer::new(parse_logfile("(number-range \"0\" ())").unwrap());

        assert_eq!(
            Vec::<TypeRef>::deserialize(des).unwrap(),
            vec![TypeRef::NumberRange(Some("0".into()), None)]
        );
    }
}
