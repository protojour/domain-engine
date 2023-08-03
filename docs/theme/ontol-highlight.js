hljs.registerLanguage("ontol", (hljs) => ({
  name: "ONTOL",
  keywords: {
    keyword: "fmt map pub rel type use as with",
    built_in: "is id gen default auto create_time update_time",
    type: "bool int float number string datetime date time regex uuid",
    literal: "true false null inf nan",
  },
  contains: [
    hljs.APOS_STRING_MODE,
    hljs.QUOTE_STRING_MODE,
    hljs.C_LINE_COMMENT_MODE,
    hljs.C_NUMBER_MODE,
  ],
}));

hljs.initHighlightingOnLoad();