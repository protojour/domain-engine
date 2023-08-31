hljs.registerLanguage("ontol", (hljs) => ({
  name: "ONTOL",
  keywords: {
    keyword: "def fmt map pub rel use as",
    built_in: "is id gen default auto create_time update_time",
    type: "boolean number integer i64 float f64 string datetime date time regex uuid",
    literal: "true false inf nan",
  },
  contains: [
    hljs.APOS_STRING_MODE,
    hljs.QUOTE_STRING_MODE,
    hljs.C_LINE_COMMENT_MODE,
    hljs.C_NUMBER_MODE,
  ],
}));

hljs.initHighlightingOnLoad();
