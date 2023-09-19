use std::sync::Arc;

use axum::Extension;
use domain_engine_juniper::{gql_scalar::GqlScalar, juniper};

pub struct GraphqlService {
    pub schema: domain_engine_juniper::Schema,
    pub service_ctx: domain_engine_juniper::context::ServiceCtx,
    pub endpoint_url: String,
}

pub async fn graphql_handler(
    Extension(service): Extension<Arc<GraphqlService>>,
    GraphQLRequest(batch_request): GraphQLRequest,
) -> (
    axum::http::StatusCode,
    axum::Json<juniper::http::GraphQLBatchResponse<GqlScalar>>,
) {
    let response = batch_request
        .execute(&service.schema, &service.service_ctx)
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

pub struct GraphQLRequest(pub juniper::http::GraphQLBatchRequest<GqlScalar>);

#[async_trait::async_trait]
impl<S: Send + Sync> axum::extract::FromRequest<S, axum::body::Body> for GraphQLRequest {
    type Rejection = (axum::http::StatusCode, String);

    async fn from_request(
        req: axum::http::Request<axum::body::Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let content_type = req.headers().get("content-type").ok_or_else(|| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "no content-type header".to_owned(),
            )
        })?;

        match content_type.to_str() {
            Ok("application/json" | "application/graphql") => {
                let graphql_request =
                    axum::Json::<juniper::http::GraphQLBatchRequest<GqlScalar>>::from_request(
                        req, state,
                    )
                    .await
                    .map_err(|err| (axum::http::StatusCode::BAD_REQUEST, err.to_string()))?;

                Ok(GraphQLRequest(graphql_request.0))
            }
            _ => Err((
                axum::http::StatusCode::BAD_REQUEST,
                "invalid content-type".to_owned(),
            )),
        }
    }
}

const GRAPHIQL: &str = r#"
<!DOCTYPE html>
<html>

<head>
    <title>Memoriam Domain GraphiQL</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="//unpkg.com/graphiql/graphiql.min.css" />
    <link rel="shortcut icon" href="/favicon.png" />
    <script crossorigin src="//unpkg.com/react/umd/react.production.min.js"></script>
    <script crossorigin src="//unpkg.com/react-dom/umd/react-dom.production.min.js"></script>
    <script crossorigin src="//unpkg.com/graphiql/graphiql.min.js"></script>
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
                defaultSecondaryEditorOpen: true,
                docExplorerOpen: true,
            }),
            document.getElementById('root'),
        )
    </script>
</body>

</html>
"#;
