hljs.registerLanguage("ontol", (hljs) => ({
  name: "ONTOL",
  keywords: {
    keyword: "def fmt map rel use as",
    rels: "is identifies id min max gen default example",
    built_in: "auto create_time update_time",
    type: "boolean number integer i64 float f32 f64 serial text uuid datetime",
    literal: "true false inf nan",
    $pattern: /\w+|[.,:?+*=<>|]+/,
    operator: ". .. , : :: ? ?: + * = | =>",
    param: "private open extern match",
  },
  contains: [
    hljs.APOS_STRING_MODE,
    hljs.QUOTE_STRING_MODE,
    hljs.C_LINE_COMMENT_MODE,
    hljs.C_NUMBER_MODE,
    {
      begin: /(\s|\{)/,
      contains: [
        {
          className: "self",
          begin: /\./
        }
      ]
    }
  ],
}));

hljs.initHighlightingOnLoad();
