hljs.registerLanguage("ontol", (hljs) => ({
  name: "ONTOL",
  keywords: {
    keyword: "def fmt map rel use as",
    rels: "is id min max gen default example",
    built_in: "auto create_time update_time",
    type: "boolean number integer i64 float f32 f64 serial text uuid datetime",
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
