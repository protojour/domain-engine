hljs.registerLanguage("ontol", (hljs) => ({
  name: "ONTOL",
  keywords: {
    keyword: "def fmt map rel use as",
    rels: "is id gen default example ",
    built_in: "auto create_time update_time",
    type: "boolean number integer i64 float f64 text datetime date time regex uuid",
    literal: "true false inf nan",
    $pattern: /\w+|[.,:?+*=<>]+|\s+/,
    operator: ". .. , : :: ? + * = | =>",
    self: ".",
    param: "private open match",
  },
  contains: [
    hljs.APOS_STRING_MODE,
    hljs.QUOTE_STRING_MODE,
    hljs.C_LINE_COMMENT_MODE,
    hljs.C_NUMBER_MODE,
  ],
}));

hljs.initHighlightingOnLoad();
