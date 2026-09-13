//! HTTP-only carbonserver endpoints. Pickle and gRPC are deliberately absent.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{MatchedPath, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::app::App;
use crate::index::Match;
use crate::protocol::{v2, v3};

const MAX_BODY: usize = 1 << 20;
const MAX_QUERY: usize = 4096;

type FindCache = (
    u64,
    usize,
    HashMap<String, (usize, Vec<Match>)>,
    VecDeque<String>,
);
type RenderCache = (
    u64,
    usize,
    HashMap<String, (usize, (Vec<v3::FetchResponse>, Vec<v2::FetchResponse>))>,
    VecDeque<String>,
);

#[derive(Clone)]
struct HttpState {
    app: Arc<App>,
    requests: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    find_cache: Arc<Mutex<FindCache>>,
    render_cache: Arc<Mutex<RenderCache>>,
    cache_bytes: usize,
}

pub fn router(app: Arc<App>) -> Router {
    let concurrent = if app.config.carbonserver.concurrent_requests == 0 {
        64
    } else {
        app.config.carbonserver.concurrent_requests
    };
    let cache_bytes = app.config.carbonserver.query_cache_size;
    let state = HttpState {
        app,
        requests: Arc::new(Semaphore::new(concurrent)),
        jobs: Arc::new(Semaphore::new(concurrent)),
        find_cache: Arc::new(Mutex::new((0, 0, HashMap::new(), VecDeque::new()))),
        render_cache: Arc::new(Mutex::new((0, 0, HashMap::new(), VecDeque::new()))),
        cache_bytes,
    };
    Router::new()
        .route("/metrics/find/", get(find).post(find))
        .route("/metrics/list/", get(list))
        .route("/metrics/list_query/", get(list_query))
        .route("/render/", get(render).post(render))
        .route("/info/", get(info).post(info))
        .route("/metrics/details/", get(details))
        .route(
            "/_internal/capabilities/",
            get(capabilities).post(capabilities),
        )
        .route("/forcescan", post(force_scan))
        .route("/admin/info", get(admin_info))
        .route("/admin/quota", get(admin_quota))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_timeout,
        ))
        .with_state(state)
}

async fn request_timeout(
    State(state): State<HttpState>,
    request: Request,
    next: middleware::Next,
) -> Response {
    let metrics = state
        .app
        .prometheus
        .as_ref()
        .and_then(|m| m.carbonserver.as_ref());
    // Only Go's instrumented routes; never use arbitrary client paths as labels.
    let guard = metrics.and_then(|metrics| {
        let path = request
            .extensions()
            .get::<MatchedPath>()?
            .as_str()
            .trim_end_matches('/');
        [
            "/metrics/find",
            "/metrics/list",
            "/metrics/list_query",
            "/metrics/details",
            "/render",
            "/info",
            "/_internal/capabilities",
        ]
        .into_iter()
        .find(|handler| *handler == path)
        .map(|handler| metrics.request(handler))
    });
    let response = match tokio::time::timeout(
        state.app.config.carbonserver.request_timeout,
        next.run(request),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => {
            if guard.is_some()
                && let Some(metrics) = metrics
            {
                metrics.timeout_requests.inc();
            }
            (StatusCode::GATEWAY_TIMEOUT, "request timed out").into_response()
        }
    };
    if let Some(guard) = guard {
        guard.finish(response.status());
    }
    response
}

type ResultResponse = Result<Response, (StatusCode, String)>;
async fn permit(
    state: &HttpState,
) -> Result<tokio::sync::OwnedSemaphorePermit, (StatusCode, String)> {
    state
        .requests
        .clone()
        .try_acquire_owned()
        .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "too many requests".into()))
}
fn format(
    headers: &HeaderMap,
    requested: Option<&str>,
) -> Result<&'static str, (StatusCode, String)> {
    if headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("application/x-carbonapi-v3-pb"))
    {
        return Ok("v3");
    }
    match requested.unwrap_or("json") {
        "json" | "carbonapi_v2_json" => Ok("json"),
        "protobuf" | "protobuf3" | "carbonapi_v2_pb" => Ok("v2"),
        "carbonapi_v3_pb" => Ok("v3"),
        _ => Err((StatusCode::BAD_REQUEST, "unsupported format".into())),
    }
}
fn encoded(
    format: &str,
    json_value: Value,
    v2: Option<Vec<u8>>,
    v3: Option<Vec<u8>>,
) -> ResultResponse {
    match format {
        "json" => Ok((
            [(header::CONTENT_TYPE, "application/json")],
            Json(json_value),
        )
            .into_response()),
        "v2" => v2
            .map(|b| ([(header::CONTENT_TYPE, "application/x-protobuf")], b).into_response())
            .ok_or((
                StatusCode::BAD_REQUEST,
                "format unsupported for endpoint".into(),
            )),
        "v3" => v3
            .map(|b| ([(header::CONTENT_TYPE, "application/x-carbonapi-v3-pb")], b).into_response())
            .ok_or((
                StatusCode::BAD_REQUEST,
                "format unsupported for endpoint".into(),
            )),
        _ => unreachable!(),
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn checked_limit(value: Option<usize>, max: usize) -> Result<usize, (StatusCode, String)> {
    let n = value.unwrap_or(max).min(max);
    if n == 0 {
        return Err((StatusCode::BAD_REQUEST, "limit must be positive".into()));
    }
    Ok(n)
}
fn matched_json(matches: &[Match]) -> Vec<Value> {
    matches
        .iter()
        .map(|m| {
            if m.is_leaf {
                json!({"path":m.path,"isLeaf":true})
            } else {
                json!({"path":m.path})
            }
        })
        .collect()
}
async fn cached_find(
    state: &HttpState,
    query: &str,
    limit: usize,
) -> Result<(Vec<Match>, bool), (StatusCode, String)> {
    let job = state
        .jobs
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "server stopping".into()))?;
    let state = state.clone();
    let query = query.to_owned();
    tokio::task::spawn_blocking(move || {
        let _job = job;
        compute_find(&state, &query, limit)
    })
    .await
    .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "find task failed".into()))?
    .map_err(bad)
}
fn compute_find(
    state: &HttpState,
    query: &str,
    limit: usize,
) -> Result<(Vec<Match>, bool), crate::index::GlobError> {
    if !state.app.config.carbonserver.find_cache_enabled || state.cache_bytes == 0 {
        return state
            .app
            .index
            .find(query, limit)
            .map(|matches| (matches, false));
    }
    let generation = state.app.index.generation();
    let key = format!("{limit}\0{query}");
    let mut cache = state.find_cache.lock().expect("find cache lock poisoned");
    if cache.0 != generation {
        cache.0 = generation;
        cache.1 = 0;
        cache.2.clear();
        cache.3.clear();
    }
    if let Some((_, matches)) = cache.2.get(&key) {
        return Ok((matches.iter().take(limit).cloned().collect(), true));
    }
    let matches = state.app.index.find(query, limit)?;
    let bytes = key.len() + matches.iter().map(|m| m.path.len() + 16).sum::<usize>();
    while cache.1.saturating_add(bytes) > state.cache_bytes {
        let Some(old) = cache.3.pop_front() else {
            break;
        };
        if let Some((old_bytes, _)) = cache.2.remove(&old) {
            cache.1 -= old_bytes;
        }
    }
    if bytes <= state.cache_bytes {
        cache.1 += bytes;
        cache.3.push_back(key.clone());
        cache.2.insert(key, (bytes, matches.clone()));
    }
    Ok((matches, false))
}
fn cached_render(
    state: &HttpState,
    key: &str,
) -> Option<(Vec<v3::FetchResponse>, Vec<v2::FetchResponse>)> {
    if !state.app.config.carbonserver.query_cache_enabled || state.cache_bytes == 0 {
        return None;
    }
    let generation = state
        .app
        .read_generation
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut cache = state
        .render_cache
        .lock()
        .expect("render cache lock poisoned");
    if cache.0 != generation {
        cache.0 = generation;
        cache.1 = 0;
        cache.2.clear();
        cache.3.clear();
    }
    cache.2.get(key).map(|(_, value)| value.clone())
}
fn store_render(
    state: &HttpState,
    generation: u64,
    key: String,
    value: (Vec<v3::FetchResponse>, Vec<v2::FetchResponse>),
) {
    if !state.app.config.carbonserver.query_cache_enabled
        || state.cache_bytes == 0
        || state
            .app
            .read_generation
            .load(std::sync::atomic::Ordering::Relaxed)
            != generation
    {
        return;
    }
    let mut cache = state
        .render_cache
        .lock()
        .expect("render cache lock poisoned");
    if cache.0 != generation {
        cache.0 = generation;
        cache.1 = 0;
        cache.2.clear();
        cache.3.clear();
    }
    if cache.2.contains_key(&key) {
        return;
    }
    let bytes = key.len()
        + value
            .0
            .iter()
            .map(|m| m.name.len() + m.path_expression.len() + m.values.len() * 8 + 96)
            .sum::<usize>()
        + value
            .1
            .iter()
            .map(|m| m.name.len() + m.values.len() * 9 + 32)
            .sum::<usize>();
    while cache.1.saturating_add(bytes) > state.cache_bytes {
        let Some(old) = cache.3.pop_front() else {
            break;
        };
        if let Some((old_bytes, _)) = cache.2.remove(&old) {
            cache.1 -= old_bytes;
        }
    }
    if bytes <= state.cache_bytes {
        cache.1 += bytes;
        cache.3.push_back(key.clone());
        cache.2.insert(key, (bytes, value));
    }
}

fn value<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}
fn values(params: &[(String, String)], name: &str) -> Vec<String> {
    params
        .iter()
        .filter(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
        .collect()
}
async fn find(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(params): Query<Vec<(String, String)>>,
    body: Bytes,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let wire = format(&headers, value(&params, "format"))?;
    let queries = if wire == "v3" && !body.is_empty() {
        v3::MultiGlobRequest::decode(body).map_err(bad)?.metrics
    } else {
        values(&params, "query")
    };
    if queries.is_empty() || queries.len() > state.app.config.carbonserver.max_globs {
        return Err((StatusCode::BAD_REQUEST, "invalid number of queries".into()));
    }
    let limit = state.app.config.carbonserver.max_metrics_globbed;
    let mut result = Vec::new();
    let mut v3_result = Vec::new();
    let mut from_cache = true;
    for name in queries {
        if name.len() > MAX_QUERY {
            return Err((StatusCode::BAD_REQUEST, "query too long".into()));
        }
        let (matches, hit) = cached_find(&state, &name, limit).await?;
        from_cache &= hit;
        result.push(json!({"name":name,"matches":matched_json(&matches)}));
        v3_result.push(v3::GlobResponse {
            name,
            matches: matches
                .into_iter()
                .map(|m| v3::GlobMatch {
                    path: m.path,
                    is_leaf: m.is_leaf,
                })
                .collect(),
        });
    }
    let v3_response = v3::MultiGlobResponse { metrics: v3_result };
    let v2_response = if v3_response.metrics.len() == 1 {
        Some(
            v2::GlobResponse {
                name: v3_response.metrics[0].name.clone(),
                matches: v3_response.metrics[0]
                    .matches
                    .iter()
                    .map(|m| v2::GlobMatch {
                        path: m.path.clone(),
                        is_leaf: m.is_leaf,
                    })
                    .collect(),
            }
            .encode_to_vec(),
        )
    } else {
        None
    };
    let response = encoded(
        wire,
        json!({"metrics":result}),
        v2_response,
        Some(v3_response.encode_to_vec()),
    )?;
    if state.app.config.carbonserver.find_cache_enabled
        && state.cache_bytes > 0
        && let Some(metrics) = state
            .app
            .prometheus
            .as_ref()
            .and_then(|m| m.carbonserver.as_ref())
    {
        metrics.cache_request("find", from_cache);
    }
    Ok(response)
}

#[derive(Deserialize)]
struct ListQuery {
    format: Option<String>,
}
async fn list(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let wire = format(&headers, query.format.as_deref())?;
    let metrics = state.app.index.list();
    encoded(
        wire,
        json!({"Metrics":metrics}),
        Some(
            v2::ListMetricsResponse {
                metrics: metrics.clone(),
            }
            .encode_to_vec(),
        ),
        Some(v3::ListMetricsResponse { metrics }.encode_to_vec()),
    )
}

#[derive(Deserialize)]
struct ListSearch {
    target: String,
    limit: Option<usize>,
    leaf_only: Option<bool>,
    stats_only: Option<bool>,
}
async fn list_query(
    State(state): State<HttpState>,
    Query(query): Query<ListSearch>,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let limit = checked_limit(query.limit, 65_536)?;
    let (matches, _) = cached_find(&state, &query.target, limit).await?;
    let leaf_only = query.leaf_only.unwrap_or(false);
    let all = if leaf_only {
        Vec::new()
    } else {
        state.app.index.list()
    };
    let mut leaves = std::collections::BTreeSet::new();
    for matched in matches {
        if matched.is_leaf {
            leaves.insert(matched.path.clone());
        }
        if !leaf_only {
            let prefix = matched.path + ".";
            leaves.extend(all.iter().filter(|m| m.starts_with(&prefix)).cloned());
        }
    }
    let count = leaves.len();
    let mut physical_size = 0u64;
    let mut logical_size = 0u64;
    let mut metrics = Vec::new();
    for name in leaves {
        let meta = state.app.index.get(&name).unwrap_or_default();
        physical_size = physical_size.saturating_add(meta.physical_size);
        logical_size = logical_size.saturating_add(meta.logical_size);
        if !query.stats_only.unwrap_or(false) && metrics.len() < limit {
            metrics.push(json!({"Name":name,"PhysicalSize":meta.physical_size as i64,"LogicalSize":meta.logical_size as i64}));
        }
    }
    Ok(Json(json!({"Count":count, "PhysicalSize":physical_size as i64,"LogicalSize":logical_size as i64,"Metrics":metrics})).into_response())
}

fn encode_render(
    wire: &str,
    v3_metrics: Vec<v3::FetchResponse>,
    v2_metrics: Vec<v2::FetchResponse>,
) -> ResultResponse {
    // Go keeps the v2 JSON format because v2 carries explicit absence flags.
    let json_metrics: Vec<_> = v2_metrics.iter().map(|m| json!({"name":m.name,"startTime":m.start_time,"stopTime":m.stop_time,"stepTime":m.step_time,"values":m.values.iter().map(|&v| proto_json_float(v)).collect::<Vec<_>>(),"isAbsent":m.is_absent})).collect();
    encoded(
        wire,
        json!({"metrics":json_metrics}),
        Some(
            v2::MultiFetchResponse {
                metrics: v2_metrics,
            }
            .encode_to_vec(),
        ),
        Some(
            v3::MultiFetchResponse {
                metrics: v3_metrics,
            }
            .encode_to_vec(),
        ),
    )
}

fn proto_json_float(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value == f64::INFINITY {
        json!("Infinity")
    } else if value == f64::NEG_INFINITY {
        json!("-Infinity")
    } else {
        json!(value)
    }
}

async fn render(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(params): Query<Vec<(String, String)>>,
    body: Bytes,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let wire = format(&headers, value(&params, "format"))?;
    let targets: Vec<(String, i64, i64, String)> = if wire == "v3" && !body.is_empty() {
        v3::MultiFetchRequest::decode(body)
            .map_err(bad)?
            .metrics
            .into_iter()
            .map(|m| {
                (
                    m.name.clone(),
                    m.start_time,
                    m.stop_time,
                    if m.path_expression.is_empty() {
                        m.name
                    } else {
                        m.path_expression
                    },
                )
            })
            .collect()
    } else {
        let from = value(&params, "from")
            .ok_or((StatusCode::BAD_REQUEST, "missing from".into()))?
            .parse::<i64>()
            .map_err(bad)?;
        let until = value(&params, "until")
            .ok_or((StatusCode::BAD_REQUEST, "missing until".into()))?
            .parse::<i64>()
            .map_err(bad)?;
        values(&params, "target")
            .into_iter()
            .map(|t| (t.clone(), from, until, t))
            .collect()
    };
    if targets.is_empty() || targets.len() > state.app.config.carbonserver.max_globs {
        return Err((StatusCode::BAD_REQUEST, "invalid targets".into()));
    }
    // Fetch clamps ranges to now and selects retention by age, even without writes.
    let fetched_at = now();
    let cache_key = format!("{fetched_at}:{targets:?}");
    if let Some((v3_metrics, v2_metrics)) = cached_render(&state, &cache_key) {
        let response = encode_render(wire, v3_metrics, v2_metrics)?;
        if let Some(metrics) = state
            .app
            .prometheus
            .as_ref()
            .and_then(|m| m.carbonserver.as_ref())
        {
            metrics.cache_request("query", true);
        }
        return Ok(response);
    }
    let render_generation = state
        .app
        .read_generation
        .load(std::sync::atomic::Ordering::Relaxed);
    // Fetches run in parallel, bounded by the jobs semaphore; results keep find order.
    let mut fetches = Vec::new();
    for (target, from, until, expression) in targets {
        if target.len() > MAX_QUERY || from >= until {
            return Err((StatusCode::BAD_REQUEST, "invalid target or range".into()));
        }
        let (found, _) = cached_find(
            &state,
            &target,
            state
                .app
                .config
                .carbonserver
                .max_metrics_rendered
                .saturating_add(1),
        )
        .await?;
        for found in found.into_iter().filter(|m| m.is_leaf) {
            if fetches.len() >= state.app.config.carbonserver.max_metrics_rendered {
                return Err((StatusCode::BAD_REQUEST, "too many rendered metrics".into()));
            }
            let app = state.app.clone();
            let name = found.path.clone();
            let job = state
                .jobs
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "server stopping".into()))?;
            let task = tokio::task::spawn_blocking(move || {
                let _job = job;
                let series = app.fetch(&name, from, until, fetched_at)?;
                let metadata = app.metadata(&name)?;
                Ok::<_, std::io::Error>((series, metadata))
            });
            fetches.push((found.path, expression.clone(), from, until, task));
        }
    }
    let mut v3_metrics = Vec::new();
    let mut v2_metrics = Vec::new();
    for (name, expression, from, until, task) in fetches {
        let series = task
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "fetch task failed".into(),
                )
            })?
            .map_err(internal)?;
        let (Some(series), meta) = series else {
            continue;
        };
        let values: Vec<f64> = series
            .values
            .iter()
            .map(|v| v.unwrap_or(f64::NAN))
            .collect();
        let absent: Vec<bool> = series.values.iter().map(Option::is_none).collect();
        let method = format!("{:?}", meta.aggregation);
        v3_metrics.push(v3::FetchResponse {
            name: name.clone(),
            path_expression: expression,
            consolidation_func: method.clone(),
            start_time: series.from,
            stop_time: series.until,
            step_time: i64::from(series.step),
            x_files_factor: meta.x_files_factor,
            high_precision_timestamps: false,
            values: values.clone(),
            applied_functions: vec![],
            request_start_time: from,
            request_stop_time: until,
        });
        v2_metrics.push(v2::FetchResponse {
            name,
            start_time: series.from as i32,
            stop_time: series.until as i32,
            step_time: series.step as i32,
            values: values
                .iter()
                .zip(&absent)
                .map(|(v, a)| if *a { 0.0 } else { *v })
                .collect(),
            is_absent: absent,
        });
    }
    store_render(
        &state,
        render_generation,
        cache_key,
        (v3_metrics.clone(), v2_metrics.clone()),
    );
    let response = encode_render(wire, v3_metrics, v2_metrics)?;
    if state.app.config.carbonserver.query_cache_enabled
        && state.cache_bytes > 0
        && let Some(metrics) = state
            .app
            .prometheus
            .as_ref()
            .and_then(|m| m.carbonserver.as_ref())
    {
        metrics.cache_request("query", false);
    }
    Ok(response)
}

async fn info(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(params): Query<Vec<(String, String)>>,
    body: Bytes,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let wire = format(&headers, value(&params, "format"))?;
    let names = if wire == "v3" && !body.is_empty() {
        v3::MultiGlobRequest::decode(body).map_err(bad)?.metrics
    } else {
        values(&params, "target")
    };
    if names.is_empty() || names.len() > state.app.config.carbonserver.max_globs {
        return Err((StatusCode::BAD_REQUEST, "invalid number of targets".into()));
    };
    let mut metrics = Vec::new();
    for name in names {
        let meta = state.app.metadata(&name).map_err(internal)?;
        let retention = meta
            .retentions
            .iter()
            .map(|r| v3::Retention {
                seconds_per_point: i64::from(r.seconds_per_point),
                number_of_points: i64::from(r.points),
            })
            .collect();
        metrics.push(v3::MetricsInfoResponse {
            name: name.clone(),
            consolidation_func: format!("{:?}", meta.aggregation),
            max_retention: meta
                .retentions
                .last()
                .map(|r| i64::from(r.seconds_per_point) * i64::from(r.points))
                .unwrap_or(0),
            x_files_factor: meta.x_files_factor,
            retentions: retention,
        });
    }
    let v3_response = v3::MultiMetricsInfoResponse { metrics };
    let first = v3_response.metrics.first().unwrap();
    let v2_response = v2::InfoResponse {
        name: first.name.clone(),
        aggregation_method: first.consolidation_func.clone(),
        max_retention: first.max_retention as i32,
        x_files_factor: first.x_files_factor,
        retentions: first
            .retentions
            .iter()
            .map(|r| v2::Retention {
                seconds_per_point: r.seconds_per_point as i32,
                number_of_points: r.number_of_points as i32,
            })
            .collect(),
    };
    let json_metrics:Vec<_>=v3_response.metrics.iter().map(|m|json!({"name":m.name,"consolidationFunc":m.consolidation_func,"maxRetention":m.max_retention.to_string(),"xFilesFactor":m.x_files_factor,"retentions":m.retentions.iter().map(|r|json!({"secondsPerPoint":r.seconds_per_point.to_string(),"numberOfPoints":r.number_of_points.to_string()})).collect::<Vec<_>>() })).collect();
    encoded(
        wire,
        json!({"metrics":json_metrics}),
        Some(v2_response.encode_to_vec()),
        Some(v3_response.encode_to_vec()),
    )
}

async fn details(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(params): Query<Vec<(String, String)>>,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let wire = format(&headers, value(&params, "format"))?;
    let names = values(&params, "metrics");
    let names = if names.is_empty() {
        state.app.index.list()
    } else {
        names
    };
    let mut out = Vec::new();
    let mut v2_metrics = HashMap::new();
    let mut v3_metrics = HashMap::new();
    for name in names {
        if let Some(meta) = state.app.index.get(&name) {
            let size = i64::try_from(meta.logical_size).unwrap_or(i64::MAX);
            let real_size = i64::try_from(meta.physical_size).unwrap_or(i64::MAX);
            out.push(json!({"Size":size,"RealSize":real_size,"Name":name}));
            v2_metrics.insert(
                name.clone(),
                v2::MetricDetails {
                    size,
                    mod_time: 0,
                    atime: 0,
                    rd_time: 0,
                },
            );
            v3_metrics.insert(
                name,
                v3::MetricDetails {
                    size,
                    mod_time: 0,
                    atime: 0,
                    rd_time: 0,
                    real_size,
                },
            );
        }
    }
    encoded(
        wire,
        json!({"Metrics":out,"FreeSpace":0,"TotalSpace":0}),
        Some(
            v2::MetricDetailsResponse {
                metrics: v2_metrics,
                free_space: 0,
                total_space: 0,
            }
            .encode_to_vec(),
        ),
        Some(
            v3::MetricDetailsResponse {
                metrics: v3_metrics,
                free_space: 0,
                total_space: 0,
            }
            .encode_to_vec(),
        ),
    )
}
async fn capabilities(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
    body: Bytes,
) -> ResultResponse {
    let _permit = permit(&state).await?;
    let wire = format(&headers, query.format.as_deref())?;
    if wire == "v2" {
        return Err((
            StatusCode::BAD_REQUEST,
            "format unsupported for endpoint".into(),
        ));
    };
    if !body.is_empty() {
        v3::CapabilityRequest::decode(body).map_err(bad)?;
    }
    let response = v3::CapabilityResponse {
        supported_protocols: vec![
            "carbonapi_v3_pb".into(),
            "carbonapi_v2_pb".into(),
            "carbonapi_v2_json".into(),
        ],
        name: "carbon-rs".into(),
        high_precision_timestamps: false,
        support_filtering_functions: false,
        like_splitted_requests: true,
        support_streaming: false,
    };
    encoded(
        wire,
        json!({"supportedProtocols":response.supported_protocols,"name":response.name,"highPrecisionTimestamps":false,"supportFilteringFunctions":false,"likeSplittedRequests":true,"supportStreaming":false}),
        None,
        Some(response.encode_to_vec()),
    )
}
async fn force_scan(State(state): State<HttpState>) -> ResultResponse {
    let _permit = permit(&state).await?;
    let app = state.app.clone();
    tokio::task::spawn_blocking(move || app.scan())
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "scan task failed".into()))?
        .map_err(internal)?;
    Ok(Json(json!({"status":"ok","generation":state.app.index.generation()})).into_response())
}
async fn admin_info(State(state): State<HttpState>) -> ResultResponse {
    let _permit = permit(&state).await?;
    Ok(Json(json!({"generation":state.app.index.generation(),"metrics":state.app.index.list().len(),"cache":state.app.cache.stats()})).into_response())
}
async fn admin_quota(State(state): State<HttpState>) -> ResultResponse {
    let _permit = permit(&state).await?;
    Ok(Json(json!({"enabled":state.app.quotas.is_some(), "namespaces":state.app.quotas.as_ref().map(|quota| quota.report()).unwrap_or_default()})).into_response())
}
fn bad(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}
fn internal(e: std::io::Error) -> (StatusCode, String) {
    let status = match e.kind() {
        std::io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        std::io::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::index::MetricMeta;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn request(addr: std::net::SocketAddr, request: Vec<u8>) -> Vec<u8> {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(&request).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn request_metrics_cover_overload_timeout_and_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let schema = dir.path().join("schemas");
        std::fs::write(&schema, "[all]\npattern = .*\nretentions = 1s:60\n").unwrap();
        let mut config = Config::default();
        config.whisper.data_dir = dir.path().join("wsp").display().to_string();
        config.whisper.schemas_file = schema.display().to_string();
        config.prometheus.enabled = true;
        config.carbonserver.enabled = true;
        config.carbonserver.request_timeout = std::time::Duration::from_millis(5);
        let app = App::new(config).unwrap();
        let state = HttpState {
            app: app.clone(),
            requests: Arc::new(Semaphore::new(0)),
            jobs: Arc::new(Semaphore::new(1)),
            find_cache: Arc::new(Mutex::new((0, 0, HashMap::new(), VecDeque::new()))),
            render_cache: Arc::new(Mutex::new((0, 0, HashMap::new(), VecDeque::new()))),
            cache_bytes: 0,
        };
        let router = Router::new()
            .route("/render/", get(render))
            .route("/info/", get(std::future::pending::<StatusCode>))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_timeout,
            ))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let metrics = app
            .prometheus
            .as_ref()
            .unwrap()
            .carbonserver
            .as_ref()
            .unwrap();
        for (path, status) in [("/render/", "429"), ("/info/", "504")] {
            let response = request(
                addr,
                format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").into_bytes(),
            )
            .await;
            assert!(response.starts_with(format!("HTTP/1.1 {status}").as_bytes()));
            assert_eq!(
                metrics
                    .requests
                    .with_label_values(&[status, path.trim_end_matches('/')])
                    .get(),
                1
            );
        }
        assert_eq!(metrics.timeout_requests.get(), 1);
        assert_eq!(metrics.cancelled_requests.get(), 0);
        assert_eq!(metrics.durations.get_sample_count(), 2);
        drop(metrics.request("/render")); // A dropped handler future must count as cancellation.
        assert_eq!(metrics.cancelled_requests.get(), 1);
        assert_eq!(metrics.durations.get_sample_count(), 3);
        task.abort();
    }
    #[tokio::test]
    async fn repeated_find_params_and_v3_wire_work() {
        let dir = tempfile::tempdir().unwrap();
        let schema = dir.path().join("schemas");
        std::fs::write(&schema, "[all]\npattern = .*\nretentions = 1s:60\n").unwrap();
        let mut config = Config::default();
        config.whisper.data_dir = dir.path().join("wsp").display().to_string();
        config.whisper.schemas_file = schema.display().to_string();
        config.carbonserver.query_cache_size = 4;
        config.carbonserver.max_metrics_rendered = 1;
        let app = App::new(config).unwrap();
        let at = now();
        for name in ["a.one", "b.two"] {
            app.ingest(
                name.into(),
                whisper_rs::Point {
                    timestamp: at - 1,
                    value: 1.0,
                },
            )
            .unwrap();
        }
        app.index.upsert("a.one.child", MetricMeta::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router(app)).await.unwrap() });
        let json = request(addr, b"GET /metrics/find/?query=a.*&query=b.* HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".to_vec()).await;
        assert!(
            String::from_utf8_lossy(&json)
                .contains("\"metrics\":[{\"matches\":[{\"isLeaf\":true,\"path\":\"a.one\"}]")
        );
        let payload = v3::MultiGlobRequest {
            metrics: vec!["a.*".into()],
            start_time: 0,
            stop_time: 0,
        }
        .encode_to_vec();
        let mut wire = format!("POST /metrics/find/?format=carbonapi_v3_pb HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        let protobuf = request(addr, wire).await;
        let body = &protobuf[protobuf.windows(4).position(|x| x == b"\r\n\r\n").unwrap() + 4..];
        let decoded = v3::MultiGlobResponse::decode(body).unwrap();
        assert_eq!(decoded.metrics[0].matches[0].path, "a.one");
        let render = request(addr, format!("GET /render/?target=a.one&target=b.two&from={}&until={at} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", at - 10).into_bytes()).await;
        assert!(render.starts_with(b"HTTP/1.1 400"));
        let list = request(addr, b"GET /metrics/list_query/?target=a.one HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".to_vec()).await;
        let body = &list[list.windows(4).position(|x| x == b"\r\n\r\n").unwrap() + 4..];
        let list: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(list["Count"], 2);
        assert_eq!(list["Metrics"][1]["Name"], "a.one.child");
        let details = request(addr, b"GET /metrics/details/?metrics=a.one&metrics=b.two&format=protobuf HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".to_vec()).await;
        assert!(details.starts_with(b"HTTP/1.1 200"));
        let body = &details[details.windows(4).position(|x| x == b"\r\n\r\n").unwrap() + 4..];
        assert_eq!(
            v2::MetricDetailsResponse::decode(body)
                .unwrap()
                .metrics
                .len(),
            2
        );
        let missing = request(
            addr,
            b"GET /info/?target=missing HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        assert!(missing.starts_with(b"HTTP/1.1 404"));
        task.abort();
    }
    #[tokio::test]
    async fn render_json_is_v2_and_protocol_absence_is_compatible() {
        assert_eq!(proto_json_float(f64::INFINITY), json!("Infinity"));
        assert_eq!(proto_json_float(f64::NEG_INFINITY), json!("-Infinity"));
        let v3_metric = v3::FetchResponse {
            name: "a".into(),
            path_expression: "a".into(),
            consolidation_func: "Average".into(),
            start_time: 1,
            stop_time: 3,
            step_time: 1,
            x_files_factor: 0.5,
            high_precision_timestamps: false,
            values: vec![1.0, f64::NAN],
            applied_functions: vec![],
            request_start_time: 1,
            request_stop_time: 3,
        };
        let v2_metric = v2::FetchResponse {
            name: "a".into(),
            start_time: 1,
            stop_time: 3,
            step_time: 1,
            values: vec![1.0, 0.0],
            is_absent: vec![false, true],
        };
        let json = encode_render("json", vec![v3_metric.clone()], vec![v2_metric.clone()]).unwrap();
        let bytes = axum::body::to_bytes(json.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = serde_json::from_slice::<Value>(&bytes).unwrap();
        assert_eq!(json["metrics"][0]["isAbsent"], json!([false, true]));
        assert_eq!(json["metrics"][0]["values"], json!([1.0, 0.0]));
        let v2 = encode_render("v2", vec![v3_metric.clone()], vec![v2_metric.clone()]).unwrap();
        let bytes = axum::body::to_bytes(v2.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            v2::MultiFetchResponse::decode(bytes).unwrap().metrics[0].is_absent,
            vec![false, true]
        );
        let v3 = encode_render("v3", vec![v3_metric], vec![v2_metric]).unwrap();
        let bytes = axum::body::to_bytes(v3.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(v3::MultiFetchResponse::decode(bytes).unwrap().metrics[0].values[1].is_nan());
    }
}
