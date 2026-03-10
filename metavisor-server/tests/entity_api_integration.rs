//! Integration tests for Entity CRUD API
//!
//! This test requires a running server.
//! Start the server first: cargo run --bin metavisor
//! Then run tests: cargo test --test entity_api_integration

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
async fn test_create_entity() {
    let client = client();

    // First create a type for the entity
    let type_def = json!({
        "entityDefs": [{
            "name": "TestTable",
            "superTypes": ["DataSet"],
            "attributeDefs": [
                {"name": "name", "typeName": "string", "isOptional": false},
                {"name": "rowCount", "typeName": "int", "isOptional": true}
            ]
        }]
    });

    // Try to create type (ignore if already exists)
    let _ = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&type_def)
        .send()
        .await;

    // Create entity
    let entity = json!({
        "typeName": "TestTable",
        "attributes": {
            "name": "test_table_1",
            "rowCount": 1000
        },
        "labels": ["test", "integration"]
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entity)
        .send()
        .await
        .expect("Failed to create entity");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create entity should return 201 or 200, got {}",
        status
    );

    let body: Value = create_response.json().await.expect("Failed to parse JSON");
    assert!(body["entity"]["guid"].is_string());
    assert_eq!(body["entity"]["typeName"], "TestTable");
}

#[tokio::test]
async fn test_create_and_get_entity() {
    let client = client();

    // Create entity
    let entity = json!({
        "typeName": "TestTable",
        "attributes": {
            "name": "test_table_get",
            "rowCount": 500
        }
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entity)
        .send()
        .await
        .expect("Failed to create entity");

    let create_body: Value = create_response.json().await.expect("Failed to parse JSON");
    let guid = create_body["entity"]["guid"]
        .as_str()
        .expect("GUID should be present");

    // Get entity by GUID
    let get_response = client
        .get(format!(
            "{}/api/metavisor/v1/entity/guid/{}",
            TEST_SERVER_URL, guid
        ))
        .send()
        .await
        .expect("Failed to get entity");

    assert_eq!(get_response.status(), 200);
    let get_body: Value = get_response.json().await.expect("Failed to parse JSON");
    assert_eq!(get_body["entity"]["guid"], guid);
    assert_eq!(get_body["entity"]["typeName"], "TestTable");
    assert_eq!(get_body["entity"]["attributes"]["name"], "test_table_get");
}

#[tokio::test]
async fn test_update_entity() {
    let client = client();

    // Create entity
    let entity = json!({
        "typeName": "TestTable",
        "attributes": {
            "name": "test_table_update",
            "rowCount": 100
        }
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entity)
        .send()
        .await
        .expect("Failed to create entity");

    let create_body: Value = create_response.json().await.expect("Failed to parse JSON");
    let guid = create_body["entity"]["guid"]
        .as_str()
        .expect("GUID should be present");

    // Update entity
    let updated_entity = json!({
        "guid": guid,
        "typeName": "TestTable",
        "attributes": {
            "name": "test_table_updated",
            "rowCount": 200
        }
    });

    let update_response = client
        .put(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&updated_entity)
        .send()
        .await
        .expect("Failed to update entity");

    assert_eq!(update_response.status(), 200);

    // Verify update
    let get_response = client
        .get(format!(
            "{}/api/metavisor/v1/entity/guid/{}",
            TEST_SERVER_URL, guid
        ))
        .send()
        .await
        .expect("Failed to get entity");

    let get_body: Value = get_response.json().await.expect("Failed to parse JSON");
    assert_eq!(
        get_body["entity"]["attributes"]["name"],
        "test_table_updated"
    );
    assert_eq!(get_body["entity"]["attributes"]["rowCount"], 200);
}

#[tokio::test]
async fn test_delete_entity() {
    let client = client();

    // Create entity
    let entity = json!({
        "typeName": "TestTable",
        "attributes": {
            "name": "test_table_delete"
        }
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entity)
        .send()
        .await
        .expect("Failed to create entity");

    let create_body: Value = create_response.json().await.expect("Failed to parse JSON");
    let guid = create_body["entity"]["guid"]
        .as_str()
        .expect("GUID should be present");

    // Delete entity
    let delete_response = client
        .delete(format!(
            "{}/api/metavisor/v1/entity/guid/{}",
            TEST_SERVER_URL, guid
        ))
        .send()
        .await
        .expect("Failed to delete entity");

    assert_eq!(delete_response.status(), 204);

    // Verify deletion
    let get_response = client
        .get(format!(
            "{}/api/metavisor/v1/entity/guid/{}",
            TEST_SERVER_URL, guid
        ))
        .send()
        .await
        .expect("Failed to get entity");

    assert_eq!(
        get_response.status(),
        404,
        "Deleted entity should return 404"
    );
}

#[tokio::test]
async fn test_get_nonexistent_entity() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/entity/guid/nonexistent-guid-xyz",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_create_entity_with_classification() {
    let client = client();

    // Create classification type
    let class_type = json!({
        "classificationDefs": [{
            "name": "TestPII",
            "description": "PII classification for testing"
        }]
    });

    let _ = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&class_type)
        .send()
        .await;

    // Create entity with classification
    let entity = json!({
        "typeName": "TestTable",
        "attributes": {
            "name": "sensitive_table"
        },
        "classifications": [
            {
                "typeName": "TestPII"
            }
        ],
        "labels": ["sensitive"]
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entity)
        .send()
        .await
        .expect("Failed to create entity");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create entity should return 201 or 200, got {}",
        status
    );

    let body: Value = create_response.json().await.expect("Failed to parse JSON");
    assert_eq!(
        body["entity"]["classifications"].as_array().unwrap().len(),
        1
    );
    assert_eq!(body["entity"]["classifications"][0]["typeName"], "TestPII");
    assert!(body["entity"]["labels"]
        .as_array()
        .unwrap()
        .contains(&json!("sensitive")));
}

#[tokio::test]
async fn test_create_entities_bulk() {
    let client = client();

    let entities = json!([
        {
            "typeName": "TestTable",
            "attributes": {
                "name": "bulk_table_1"
            }
        },
        {
            "typeName": "TestTable",
            "attributes": {
                "name": "bulk_table_2"
            }
        }
    ]);

    let create_response = client
        .post(format!("{}/api/metavisor/v1/entity/bulk", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entities)
        .send()
        .await
        .expect("Failed to create entities");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create entities should return 201 or 200, got {}",
        status
    );

    let body: Value = create_response.json().await.expect("Failed to parse JSON");
    let entities = body["entities"]
        .as_array()
        .expect("entities should be an array");
    assert_eq!(entities.len(), 2);

    // Each entity should have a GUID
    for entity in entities {
        assert!(entity["guid"].is_string());
    }
}
