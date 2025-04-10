use std::sync::Arc;

use axum::Extension;
use domain_engine_core::DomainEngine;
use domain_engine_graphql::{domain::context::ServiceCtx, gql_scalar::GqlScalar, juniper};
use juniper_axum::extract::JuniperRequest;

pub struct GraphqlService {
    pub schema: domain_engine_graphql::domain::DomainSchema,
    pub domain_engine: Arc<DomainEngine>,
}

pub async fn domain_graphql_handler(
    Extension(service): Extension<Arc<GraphqlService>>,
    JuniperRequest(batch_request): JuniperRequest<GqlScalar>,
) -> (
    axum::http::StatusCode,
    axum::Json<juniper::http::GraphQLBatchResponse<GqlScalar>>,
) {
    let response = batch_request
        .execute(
            &service.schema,
            &ServiceCtx::from(service.domain_engine.clone()),
        )
        .await;

    (
        if response.is_ok() {
            axum::http::StatusCode::OK
        } else {
            axum::http::StatusCode::BAD_REQUEST
        },
        axum::Json(response),
    )
}

pub async fn graphiql_handler() -> axum::response::Html<&'static str> {
    axum::response::Html(GRAPHIQL)
}

const GRAPHIQL: &str = r#"
<!DOCTYPE html>
<html>

<head>
    <title>Ontool GraphiQL</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="shortcut icon" href="/favicon.png" />
    <link rel="stylesheet" href="//cdn.jsdelivr.net/npm/graphiql/graphiql.min.css" />
    <script crossorigin src="//cdn.jsdelivr.net/npm/react/umd/react.production.min.js"></script>
    <script crossorigin src="//cdn.jsdelivr.net/npm/react-dom/umd/react-dom.production.min.js"></script>
    <script crossorigin src="//cdn.jsdelivr.net/npm/graphiql/graphiql.min.js"></script>
    <style>
        body {
            margin: 0;
            padding: 0;
        }

        #root {
            height: 100vh;
        }

        .graphiql-container .CodeMirror {
            font-size: 15px;
        }
    </style>
</head>

<body>
    <div id="root"></div>
    <script>
        const fetcher = GraphiQL.createFetcher({ url: window.location })
        ReactDOM.render(
            React.createElement(GraphiQL, {
                fetcher,
                visiblePlugin: ['Documentation Explorer']
            }),
            document.getElementById('root'),
        )
    </script>
    <script>
        const socket = new WebSocket("ws://" + window.location.host + "/ws");
        socket.addEventListener("message", (e) => {
            if (e.data === "reload") {
                document.querySelector('[aria-label="Re-fetch GraphQL schema"]').click();
            }
        });
    </script>
</body>

</html>
"#;
