//! Integration tests for Type CRUD API
//!
//! This test requires a running server.
//! Start the server first: cargo run --bin metavisor
//! Or run with: cargo run --bin metavisor
//! Then run tests: cargo test --test type_api_integration

use reqwest::Client;
use serde_json::{json, Value};
use std::fs;
use std::time::Duration;

const TEST_SERVER_URL: &str = "http://localhost:31000";

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client")
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_health_check() {
    let client = client();

    let response = client
        .get(format!("{}/health", TEST_SERVER_URL))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);
    let body = response.text().await.expect("Failed to read body");
    assert_eq!(body, "OK");
}

#[tokio::test]
async fn test_api_info() {
    let client = client();

    let response = client
        .get(format!("{}/api/metavisor/v1", TEST_SERVER_URL))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_get_all_types() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert!(body.is_object());
}

#[tokio::test]
async fn test_list_type_headers() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedefs/headers",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert!(body.is_array());
}

#[tokio::test]
async fn test_type_crud_lifecycle() {
    let client = client();

    // Use a unique type name to avoid conflicts with other tests
    let type_name = format!("column_meta_{}", std::process::id());

    // 1. Create type from JSON file
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()
        .expect("Failed to find workspace root");
    let json_path = workspace_root.join("tests/data/column_meta_type.json");
    let type_json = fs::read_to_string(&json_path).expect("Failed to read JSON file");

    // Replace the type name with unique name
    let type_json = type_json.replace("column_meta", &type_name);

    let create_response = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .body(type_json.clone())
        .send()
        .await
        .expect("Failed to create type");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create type should return 201 or 200, got {}",
        status
    );

    // 2. Get type by name
    let get_response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedef/name/{}",
            TEST_SERVER_URL, type_name
        ))
        .send()
        .await
        .expect("Failed to get type");

    assert_eq!(get_response.status(), 200);
    let type_body: Value = get_response.json().await.expect("Failed to parse JSON");
    assert!(type_body["entityDefs"].is_array());

    // 3. Get type headers
    let headers_response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedefs/headers",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to get type headers");

    assert_eq!(headers_response.status(), 200);

    // 4. Update type
    let update_json = json!({
        "entityDefs": [{
            "name": type_name,
            "superTypes": ["DataSet"],
            "attributeDefs": [
                {"name": "column_id", "typeName": "string", "isOptional": false},
                {"name": "column_name", "typeName": "string", "isOptional": false},
                {"name": "new_test_attribute", "typeName": "string", "isOptional": true}
            ]
        }]
    });

    let update_response = client
        .put(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&update_json)
        .send()
        .await
        .expect("Failed to update type");

    assert_eq!(update_response.status(), 200);

    // 5. Verify update - check for new attribute
    let verify_response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedef/name/{}",
            TEST_SERVER_URL, type_name
        ))
        .send()
        .await
        .expect("Failed to verify update");

    assert_eq!(verify_response.status(), 200);
    let verify_body: Value = verify_response.json().await.expect("Failed to parse JSON");
    let entity_def = &verify_body["entityDefs"][0];
    let attr_names: Vec<&str> = entity_def["attributeDefs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert!(
        attr_names.contains(&"new_test_attribute"),
        "Updated type should contain new_test_attribute"
    );

    // 6. Delete type
    let delete_json = json!({
        "entityDefs": [{"name": type_name}]
    });

    let delete_response = client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .expect("Failed to delete type");

    assert_eq!(delete_response.status(), 204);

    // 7. Verify deletion
    let verify_delete_response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedef/name/{}",
            TEST_SERVER_URL, type_name
        ))
        .send()
        .await
        .expect("Failed to verify deletion");

    assert_eq!(
        verify_delete_response.status(),
        404,
        "Deleted type should return 404"
    );
}

#[tokio::test]
async fn test_create_enum_type() {
    let client = client();

    let enum_type = json!({
        "enumDefs": [{
            "name": "test_status",
            "description": "Test status enum",
            "elementDefs": [
                {"value": "ACTIVE", "ordinal": 1},
                {"value": "INACTIVE", "ordinal": 2}
            ],
            "defaultValue": "ACTIVE"
        }]
    });

    let create_response = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&enum_type)
        .send()
        .await
        .expect("Failed to create enum type");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create enum type should return 201 or 200, got {}",
        status
    );

    // Cleanup
    let delete_json = json!({
        "enumDefs": [{"name": "test_status"}]
    });
    client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn test_create_classification_type() {
    let client = client();

    let classification_type = json!({
        "classificationDefs": [{
            "name": "test_pii",
            "description": "PII classification for testing",
            "superTypes": [],
            "attributeDefs": [
                {"name": "sensitivity", "typeName": "string", "isOptional": true}
            ]
        }]
    });

    let create_response = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&classification_type)
        .send()
        .await
        .expect("Failed to create classification type");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create classification type should return 201 or 200, got {}",
        status
    );

    // Cleanup
    let delete_json = json!({
        "classificationDefs": [{"name": "test_pii"}]
    });
    client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn test_create_struct_type() {
    let client = client();

    let struct_type = json!({
        "structDefs": [{
            "name": "test_address",
            "description": "Address struct for testing",
            "attributeDefs": [
                {"name": "street", "typeName": "string", "isOptional": false},
                {"name": "city", "typeName": "string", "isOptional": false},
                {"name": "zip", "typeName": "string", "isOptional": true}
            ]
        }]
    });

    let create_response = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&struct_type)
        .send()
        .await
        .expect("Failed to create struct type");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create struct type should return 201 or 200, got {}",
        status
    );

    // Cleanup
    let delete_json = json!({
        "structDefs": [{"name": "test_address"}]
    });
    client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn test_create_relationship_type() {
    let client = client();

    let relationship_type = json!({
        "relationshipDefs": [{
            "name": "test_table_columns",
            "description": "Table to columns relationship",
            "relationshipCategory": "COMPOSITION",
            "propagateTags": "ONE_TO_TWO",
            "endDef1": {
                "type": "DataSet",
                "name": "columns",
                "cardinality": "SET"
            },
            "endDef2": {
                "type": "DataSet",
                "name": "table",
                "cardinality": "SINGLE"
            }
        }]
    });

    let create_response = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&relationship_type)
        .send()
        .await
        .expect("Failed to create relationship type");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create relationship type should return 201 or 200, got {}",
        status
    );

    // Cleanup
    let delete_json = json!({
        "relationshipDefs": [{"name": "test_table_columns"}]
    });
    client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn test_duplicate_type_creation() {
    let client = client();

    let entity_type = json!({
        "entityDefs": [{
            "name": "test_duplicate",
            "attributeDefs": [
                {"name": "id", "typeName": "string", "isOptional": false}
            ]
        }]
    });

    // First creation should succeed
    let create_response1 = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&entity_type)
        .send()
        .await
        .expect("Failed to create type");

    let status = create_response1.status();
    assert!(
        status == 201 || status == 200,
        "First creation should succeed, got {}",
        status
    );

    // Second creation - behavior depends on implementation (may return 201 for upsert or 409 for conflict)
    let create_response2 = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&entity_type)
        .send()
        .await
        .expect("Failed to send request");

    // Accept either 201 (upsert) or 409 (conflict) depending on implementation
    let status = create_response2.status();
    assert!(
        status == 201 || status == 200 || status == 409,
        "Second creation should return 201, 200, or 409, got {}",
        status
    );

    // Cleanup
    let delete_json = json!({
        "entityDefs": [{"name": "test_duplicate"}]
    });
    client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn test_get_nonexistent_type() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/types/typedef/name/nonexistent_type_xyz",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_update_nonexistent_type() {
    let client = client();

    let update_json = json!({
        "entityDefs": [{
            "name": "nonexistent_type_xyz",
            "attributeDefs": [
                {"name": "id", "typeName": "string", "isOptional": false}
            ]
        }]
    });

    let response = client
        .put(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&update_json)
        .send()
        .await
        .expect("Failed to send request");

    // Accept either 200 (upsert) or 404 (not found) depending on implementation
    let status = response.status();
    assert!(
        status == 200 || status == 201 || status == 404,
        "Update nonexistent type should return 200, 201, or 404, got {}",
        status
    );
}

#[tokio::test]
async fn test_delete_nonexistent_type() {
    let client = client();

    let delete_json = json!({
        "entityDefs": [{"name": "nonexistent_type_xyz"}]
    });

    let response = client
        .delete(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&delete_json)
        .send()
        .await
        .expect("Failed to send request");

    // Delete of non-existent type might return 404 or 204 depending on implementation
    assert!(
        response.status() == 404 || response.status() == 204,
        "Delete non-existent type should return 404 or 204, got {}",
        response.status()
    );
}
