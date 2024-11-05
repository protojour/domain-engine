hljs.registerLanguage("ontol", (hljs) => ({
  name: "ONTOL",
  keywords: {
    keyword: "domain def sym fmt map rel arc use as",
    rels: "is id min max gen repr format default example store_key order direction",
    built_in: "auto create_time update_time",
    type: "boolean number integer i64 float f32 f64 octets hex base64 serial text uuid ulid datetime crdt vertex",
    literal: "true false",
    $pattern: /@?\w+|[.,?+*/=<>-]+|[:|]/,
    punctuation: ". , - + : :: * / := =",
    operator: ".. ? | =>",
    param: "@private @open @extern @match @in @all_in @contains_all @intersects @equals",
  },
  contains: [
    hljs.APOS_STRING_MODE,
    hljs.QUOTE_STRING_MODE,
    hljs.C_LINE_COMMENT_MODE,
    hljs.REGEXP_MODE,
    {
      begin: /(\s|{)/,
      contains: [
        {
          className: "operator",
          begin: /\.\./,
          relevance: 1,
        },
        {
          className: "self",
          begin: /\./,
          relevance: 0,
        }
      ]
    },
    {
      begin: /\b\d\b/,
      className: "literal",
      contains: [
        {
          begin: /\.\./,
          className: "operator",
        },
        {
          begin: /\d/,
          className: "literal"
        }
      ]
    },
    hljs.C_NUMBER_MODE,
  ],
}));

hljs.initHighlightingOnLoad();
