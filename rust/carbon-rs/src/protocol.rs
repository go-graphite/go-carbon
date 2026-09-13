//! CarbonAPI protobuf messages used by carbonserver's HTTP endpoints.
//! These are wire compatible with the vendored carbonapi v2 and v3 schemas.

use prost::Message;
use serde::{Deserialize, Serialize};

pub mod v2 {
    use super::*;
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct FetchRequest {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(int32, tag = "2")]
        pub start_time: i32,
        #[prost(int32, tag = "3")]
        pub stop_time: i32,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiFetchRequest {
        #[prost(message, repeated, tag = "1")]
        pub metrics: Vec<FetchRequest>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct FetchResponse {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(int32, tag = "2")]
        pub start_time: i32,
        #[prost(int32, tag = "3")]
        pub stop_time: i32,
        #[prost(int32, tag = "4")]
        pub step_time: i32,
        #[prost(double, repeated, tag = "5")]
        pub values: Vec<f64>,
        #[prost(bool, repeated, tag = "6")]
        pub is_absent: Vec<bool>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiFetchResponse {
        #[prost(message, repeated, tag = "1")]
        pub metrics: Vec<FetchResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct GlobRequest {
        #[prost(string, tag = "1")]
        pub query: String,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct GlobMatch {
        #[prost(string, tag = "1")]
        pub path: String,
        #[prost(bool, tag = "2")]
        pub is_leaf: bool,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct GlobResponse {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(message, repeated, tag = "2")]
        pub matches: Vec<GlobMatch>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct InfoRequest {
        #[prost(string, tag = "1")]
        pub name: String,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct Retention {
        #[prost(int32, tag = "1")]
        pub seconds_per_point: i32,
        #[prost(int32, tag = "2")]
        pub number_of_points: i32,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct InfoResponse {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub aggregation_method: String,
        #[prost(int32, tag = "3")]
        pub max_retention: i32,
        #[prost(float, tag = "4")]
        pub x_files_factor: f32,
        #[prost(message, repeated, tag = "5")]
        pub retentions: Vec<Retention>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct ServerInfoResponse {
        #[prost(string, tag = "1")]
        pub server: String,
        #[prost(message, optional, tag = "2")]
        pub info: Option<InfoResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct ZipperInfoResponse {
        #[prost(message, repeated, tag = "1")]
        pub responses: Vec<ServerInfoResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct ListMetricsResponse {
        #[prost(string, repeated, tag = "1")]
        pub metrics: Vec<String>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MetricDetails {
        #[prost(int64, tag = "2")]
        pub size: i64,
        #[prost(int64, tag = "3")]
        pub mod_time: i64,
        #[prost(int64, tag = "4")]
        pub atime: i64,
        #[prost(int64, tag = "5")]
        pub rd_time: i64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MetricDetailsResponse {
        #[prost(map = "string, message", tag = "1")]
        pub metrics: std::collections::HashMap<String, MetricDetails>,
        #[prost(uint64, tag = "2")]
        pub free_space: u64,
        #[prost(uint64, tag = "3")]
        pub total_space: u64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct ProtocolVersionResponse {
        #[prost(string, tag = "1")]
        pub version: String,
    }
}

pub mod v3 {
    use super::*;
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct FilteringFunction {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, repeated, tag = "2")]
        pub arguments: Vec<String>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct CapabilityRequest {}
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct CapabilityResponse {
        #[prost(string, repeated, tag = "1")]
        pub supported_protocols: Vec<String>,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(bool, tag = "3")]
        pub high_precision_timestamps: bool,
        #[prost(bool, tag = "4")]
        pub support_filtering_functions: bool,
        #[prost(bool, tag = "5")]
        pub like_splitted_requests: bool,
        #[prost(bool, tag = "6")]
        pub support_streaming: bool,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct FetchRequest {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(int64, tag = "2")]
        pub start_time: i64,
        #[prost(int64, tag = "3")]
        pub stop_time: i64,
        #[prost(bool, tag = "4")]
        pub high_precision_timestamps: bool,
        #[prost(string, tag = "5")]
        pub path_expression: String,
        #[prost(message, repeated, tag = "6")]
        pub filter_functions: Vec<FilteringFunction>,
        #[prost(int64, tag = "7")]
        pub max_data_points: i64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiFetchRequest {
        #[prost(message, repeated, tag = "1")]
        pub metrics: Vec<FetchRequest>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct FetchResponse {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub path_expression: String,
        #[prost(string, tag = "3")]
        pub consolidation_func: String,
        #[prost(int64, tag = "4")]
        pub start_time: i64,
        #[prost(int64, tag = "5")]
        pub stop_time: i64,
        #[prost(int64, tag = "6")]
        pub step_time: i64,
        #[prost(float, tag = "7")]
        pub x_files_factor: f32,
        #[prost(bool, tag = "8")]
        pub high_precision_timestamps: bool,
        #[prost(double, repeated, tag = "9")]
        pub values: Vec<f64>,
        #[prost(string, repeated, tag = "10")]
        pub applied_functions: Vec<String>,
        #[prost(int64, tag = "11")]
        pub request_start_time: i64,
        #[prost(int64, tag = "12")]
        pub request_stop_time: i64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiFetchResponse {
        #[prost(message, repeated, tag = "1")]
        pub metrics: Vec<FetchResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiGlobRequest {
        #[prost(string, repeated, tag = "1")]
        pub metrics: Vec<String>,
        #[prost(int64, tag = "2")]
        pub start_time: i64,
        #[prost(int64, tag = "3")]
        pub stop_time: i64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct GlobMatch {
        #[prost(string, tag = "1")]
        pub path: String,
        #[prost(bool, tag = "2")]
        pub is_leaf: bool,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct GlobResponse {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(message, repeated, tag = "2")]
        pub matches: Vec<GlobMatch>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiGlobResponse {
        #[prost(message, repeated, tag = "1")]
        pub metrics: Vec<GlobResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MetricsInfoRequest {
        #[prost(string, tag = "1")]
        pub name: String,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiMetricsInfoRequest {
        #[prost(string, repeated, tag = "1")]
        pub names: Vec<String>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct Retention {
        #[prost(int64, tag = "1")]
        pub seconds_per_point: i64,
        #[prost(int64, tag = "2")]
        pub number_of_points: i64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MetricsInfoResponse {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub consolidation_func: String,
        #[prost(int64, tag = "3")]
        pub max_retention: i64,
        #[prost(float, tag = "4")]
        pub x_files_factor: f32,
        #[prost(message, repeated, tag = "5")]
        pub retentions: Vec<Retention>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiMetricsInfoResponse {
        #[prost(message, repeated, tag = "1")]
        pub metrics: Vec<MetricsInfoResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct ZipperInfoResponse {
        #[prost(map = "string, message", tag = "1")]
        pub info: std::collections::HashMap<String, MultiMetricsInfoResponse>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct ListMetricsResponse {
        #[prost(string, repeated, tag = "1")]
        pub metrics: Vec<String>,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MetricDetails {
        #[prost(int64, tag = "2")]
        pub size: i64,
        #[prost(int64, tag = "3")]
        pub mod_time: i64,
        #[prost(int64, tag = "4")]
        pub atime: i64,
        #[prost(int64, tag = "5")]
        pub rd_time: i64,
        #[prost(int64, tag = "6")]
        pub real_size: i64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MetricDetailsResponse {
        #[prost(map = "string, message", tag = "1")]
        pub metrics: std::collections::HashMap<String, MetricDetails>,
        #[prost(uint64, tag = "2")]
        pub free_space: u64,
        #[prost(uint64, tag = "3")]
        pub total_space: u64,
    }
    #[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
    pub struct MultiDetailsResponse {
        #[prost(map = "string, message", tag = "1")]
        pub metrics: std::collections::HashMap<String, MetricDetailsResponse>,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn v2_glob_wire_tags_match_schema() {
        let response = v2::GlobResponse {
            name: "a".into(),
            matches: vec![v2::GlobMatch {
                path: "b".into(),
                is_leaf: true,
            }],
        };
        assert_eq!(
            response.encode_to_vec(),
            vec![10, 1, b'a', 18, 5, 10, 1, b'b', 16, 1]
        );
        assert_eq!(
            v2::GlobResponse::decode(response.encode_to_vec().as_slice()).unwrap(),
            response
        );
    }
    #[test]
    fn v3_find_wire_round_trip() {
        let request = v3::MultiGlobRequest {
            metrics: vec!["a.*".into()],
            start_time: 3,
            stop_time: 4,
        };
        assert_eq!(
            v3::MultiGlobRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
            request
        );
    }
}
