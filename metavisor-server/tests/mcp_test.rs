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

/// Parse SSE response and extract the JSON result
fn parse_sse_response(body: &str) -> Option<Value> {
    for line in body.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if !data.is_empty() {
                if let Ok(json) = serde_json::from_str::<Value>(data) {
                    return Some(json);
                }
            }
        }
    }
    None
}

/// Initialize a session and return the session ID
async fn init_session(client: &Client) -> String {
    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0"
                }
            }
        }))
        .send()
        .await
        .expect("Failed to send initialize request");

    assert_eq!(response.status(), 200, "Initialize request should succeed");

    let session_id = response
        .headers()
        .get("mcp-session-id")
        .expect("Missing mcp-session-id header")
        .to_str()
        .expect("Invalid session ID")
        .to_string();

    // Send initialized notification
    let _ = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .send()
        .await;

    session_id
}

#[tokio::test]
async fn test_mcp_initialize() {
    let client = client();

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0"
                }
            }
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    // Check for session ID header
    assert!(response.headers().contains_key("mcp-session-id"));

    let body = response.text().await.expect("Failed to read response body");
    let json = parse_sse_response(&body).expect("Failed to parse SSE response");

    assert_eq!(json["jsonrpc"], "2.0");
    assert_eq!(json["id"], 1);
    assert!(json["result"]["capabilities"].is_object());
    assert!(json["result"]["serverInfo"]["name"].is_string());
}

#[tokio::test]
async fn test_mcp_list_tools() {
    let client = client();

    // First initialize a session
    let session_id = init_session(&client).await;

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body = response.text().await.expect("Failed to read response body");
    let json = parse_sse_response(&body)
        .unwrap_or_else(|| panic!("Failed to parse SSE response: {}", body));

    assert_eq!(json["jsonrpc"], "2.0");
    assert!(json["result"]["tools"].is_array());
    let tools = json["result"]["tools"].as_array().unwrap();
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

    // First initialize a session
    let session_id = init_session(&client).await;

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
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

    let body = response.text().await.expect("Failed to read response body");
    let json = parse_sse_response(&body)
        .unwrap_or_else(|| panic!("Failed to parse SSE response: {}", body));

    assert_eq!(json["jsonrpc"], "2.0");
    assert!(json["result"]["content"].is_array());
    assert_eq!(json["result"]["isError"], false);
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

    // Initialize a session
    let session_id = init_session(&client).await;

    // Search for entities
    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
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

    let body = response.text().await.expect("Failed to read response body");
    let json = parse_sse_response(&body)
        .unwrap_or_else(|| panic!("Failed to parse SSE response: {}", body));

    assert_eq!(json["jsonrpc"], "2.0");
    assert!(json["result"]["content"].is_array());
}

#[tokio::test]
async fn test_mcp_ping() {
    let client = client();

    // First initialize a session
    let session_id = init_session(&client).await;

    let response = client
        .post(format!("{}/mcp", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "ping"
        }))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);
}
