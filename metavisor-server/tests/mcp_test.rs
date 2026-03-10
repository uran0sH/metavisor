//! Integration tests for MCP API
//!
//! This test requires a running server.
//! Start the server first: cargo run --bin metavisor
//! Then run tests: cargo test --test mcp_test

use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

const TEST_SERVER_URL: &str = "http://localhost:31000";

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client")
}

#[tokio::test]
async fn test_mcp_initialize() {
    let client = client();

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert!(body["result"]["capabilities"].is_object());
    assert!(body["result"]["serverInfo"]["name"].is_string());
}

#[tokio::test]
async fn test_mcp_list_tools() {
    let client = client();

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert!(body["result"]["tools"].is_array());
    let tools = body["result"]["tools"].as_array().unwrap();
    assert!(!tools.is_empty(), "Should have at least one tool");

    let tool_names: Vec<&str> = tools
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(tool_names.contains(&"search_entities"));
    assert!(tool_names.contains(&"get_entity"));
    assert!(tool_names.contains(&"list_types"));
}

#[tokio::test]
async fn test_mcp_list_types_tool() {
    let client = client();

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "list_types",
                "arguments": {}
            }
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert!(body["result"]["content"].is_array());
    assert_eq!(body["result"]["isError"], false);
}

#[tokio::test]
async fn test_mcp_search_entities() {
    let client = client();

    // First create a type for testing
    let type_def = json!({
        "entityDefs": [{
            "name": "MCPTestTable",
            "superTypes": ["DataSet"],
            "attributeDefs": [
                {"name": "name", "typeName": "string", "isOptional": false}
            ]
        }]
    });

    let _ = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&type_def)
        .send()
        .await;

    // Search for entities
    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "search_entities",
                "arguments": {
                    "type_name": "MCPTestTable"
                }
            }
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert!(body["result"]["content"].is_array());
}

#[tokio::test]
async fn test_mcp_ping() {
    let client = client();

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "ping"
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 5);
}
