hljs.registerLanguage("graphql", (hljs) => ({
  name: "GraphQL",
  case_insensitive: true,
  keywords: {
    type: "type input query mutation subscription schema directive interface union scalar fragment enum on ID String Int",
    keyword: "",
    literal: "true false null",
    operator: ": !",
    $pattern: /\w+|[#.:!\s]+/,
  },
  contains: [
    hljs.HASH_COMMENT_MODE,
    hljs.QUOTE_STRING_MODE,
    hljs.NUMBER_MODE,
    {
      begin: /^(input|type)/,
      end: /}/,
      case_insensitive: false,
      excludeBegin: true,
      excludeEnd: true,
      contains: [
        {
          begin: /^input/,
          className: "type", // "gql-input emphasis",
          case_insensitive: false,
          relevance: 1,
        },
        {
          begin: /^type/,
          className: "type",
          case_insensitive: false,
          relevance: 1,
        },
        {
          begin: /[a-zA-Z_]+/,
          className: "title",
          relevance: 0,
        },
        {
          begin: /\{/,
          end: /\}/,
          contains: [
            {
              begin: /\(/,
              end: /\)/,
              contains: [
                {
                  begin: /[a-z][a-zA-Z_]+/,
                  case_insensitive: false,
                  className: "param",
                  relevance: 0,
                },
                {
                  begin: /: \[?/,
                  excludeBegin: true,
                  end: /[a-zA-Z_]+/,
                  className: "type",
                  relevance: 1,
                },
                {
                  begin: /!/,
                  className: "operator",
                  relevance: 2,
                },
              ],
            },
            {
              begin: /: \[?/,
              excludeBegin: true,
              end: /[a-zA-Z_]+/,
              className: "type",
              relevance: 1,
            },
            {
              begin: /!/,
              className: "operator",
              relevance: 2,
            },
          ],
        },
      ],
    },
  ],
}));

hljs.initHighlightingOnLoad();
