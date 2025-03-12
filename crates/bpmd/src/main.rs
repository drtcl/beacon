#![allow(unused_imports)]

use tower_http::services::ServeDir;
use axum::response::Html;
use std::fmt::Write;
use std::sync::Arc;
use axum::extract::Path;
use axum::extract::State;
use axum::{
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use axum::handler::HandlerWithoutStateExt;

type PkgName = String;
type Version = String;
type VersionList = Vec<Version>;
type PackageList = Vec<(PkgName, VersionList)>;

#[derive(Debug)]
struct PkgList {
    pkgs: PackageList,
}

fn fake_package_list() -> PkgList {
    PkgList {
        pkgs: vec![
            ("foo".into(), vec![
             "1.0.0".into(),
             "1.1.0".into(),
             "1.2.1".into(),
             "2.0.0".into(),
            ]),
            ("bar".into(), vec![
             "0.1.0".into(),
             "3.1.4".into(),
            ]),
        ],
    }
}

async fn handle404() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "Not Found")
}

#[tokio::main]
async fn main() {

    // initialize tracing
    tracing_subscriber::fmt::init();

    let pkg_list = Arc::new(fake_package_list());

    let serve_files = ServeDir::new("files").not_found_service(handle404.into_service());

    let routes = Router::new()
        .route("/", get(root))
        .route("/pkg", get(pkg))
        .route("/pkg/:name", get(pkg_name))
        .route("/pkg/:name/:version", get(pkg_name_version))
        .with_state(pkg_list)
        .nest_service("/files", serve_files)
    ;

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = SocketAddr::from(([127, 0, 0, 1], 3006));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(routes.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
#[tracing::instrument]
async fn root() -> impl IntoResponse {
    Html("<h1><a href=\"/pkg\">packages</a></h1>")
}

#[tracing::instrument]
async fn pkg(state: State<Arc<PkgList>>) -> impl IntoResponse {
    let mut body = String::new();
    for (name, _versions) in &state.pkgs {
        let _ = body.write_fmt(format_args!("<a href=\'/pkg/{name}\'>{name}</a><br>\n"));
        //let _ = body.write_fmt(format_args!("<a href=\'{name}\'>{name}</a><br>\n"));
    }
    Html(body)
}

#[tracing::instrument]
async fn pkg_name(state: State<Arc<PkgList>>, Path(name): Path<String>) -> impl IntoResponse {
    let mut body = String::new();
    for (n, versions) in &state.pkgs {
        if &name == n {
            for v in versions {
                let _ = body.write_fmt(format_args!("<a href=\'/files/{n}-{v}.bpm\'>{n}-{v}.bpm</a><br>"));
                //let _ = body.write_fmt(format_args!("<a href=\'/pkg/{n}/{v}\'>info</a><br>\n"));
            }
        }
    }
    Html(body)
}

#[derive(Debug, Deserialize)]
struct PkgVersionPath {
    name: String,
    version: String,
}

#[tracing::instrument]
async fn pkg_name_version(state: State<Arc<PkgList>>, Path(p): Path<PkgVersionPath>) -> impl IntoResponse {
    format!("Package: {}\nVersion: {}\n", p.name, p.version)
}
