//! Integration tests for Relationship CRUD API
//!
//! This test requires a running server.
//! Start the server first: cargo run --bin metavisor
//! Then run tests: cargo test --test relationship_api_integration

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

/// Helper: Create entity types needed for relationships
async fn ensure_entity_types(client: &Client) {
    // Create Table entity type
    let table_type = json!({
        "entityDefs": [{
            "name": "RelTestTable",
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
        .json(&table_type)
        .send()
        .await;

    // Create Column entity type
    let column_type = json!({
        "entityDefs": [{
            "name": "RelTestColumn",
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
        .json(&column_type)
        .send()
        .await;
}

/// Helper: Create relationship type
async fn ensure_relationship_type(client: &Client) {
    let rel_type = json!({
        "relationshipDefs": [{
            "name": "rel_test_table_columns",
            "relationshipCategory": "COMPOSITION",
            "propagateTags": "ONE_TO_TWO",
            "endDef1": {
                "type": "RelTestTable",
                "name": "columns",
                "cardinality": "SET"
            },
            "endDef2": {
                "type": "RelTestColumn",
                "name": "table",
                "cardinality": "SINGLE"
            }
        }]
    });

    let _ = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(&rel_type)
        .send()
        .await;
}

/// Helper: Create test entities and return their GUIDs
async fn create_test_entities(client: &Client) -> (String, String) {
    // Create table entity
    let table = json!({
        "typeName": "RelTestTable",
        "attributes": {
            "name": "test_table_rel"
        }
    });

    let response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&table)
        .send()
        .await
        .expect("Failed to create table entity");

    let body: Value = response.json().await.expect("Failed to parse JSON");
    let table_guid = body["entity"]["guid"]
        .as_str()
        .expect("Missing table GUID")
        .to_string();

    // Create column entity
    let column = json!({
        "typeName": "RelTestColumn",
        "attributes": {
            "name": "test_column_rel"
        }
    });

    let response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&column)
        .send()
        .await
        .expect("Failed to create column entity");

    let body: Value = response.json().await.expect("Failed to parse JSON");
    let column_guid = body["entity"]["guid"]
        .as_str()
        .expect("Missing column GUID")
        .to_string();

    (table_guid, column_guid)
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
async fn test_create_and_get_relationship() {
    let client = client();

    // Setup
    ensure_entity_types(&client).await;
    ensure_relationship_type(&client).await;
    let (table_guid, column_guid) = create_test_entities(&client).await;

    // Create relationship
    let relationship = json!({
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "test_get_rel"
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await
        .expect("Failed to create relationship");

    let status = create_response.status();
    assert!(
        status == 201 || status == 200,
        "Create relationship should return 201 or 200, got {}",
        status
    );

    let create_body: Value = create_response
        .json()
        .await
        .expect("Failed to parse create JSON");
    let rel_guid = create_body["relationship"]["guid"]
        .as_str()
        .expect("Missing relationship GUID");
    assert_eq!(
        create_body["relationship"]["typeName"],
        "rel_test_table_columns"
    );
    assert_eq!(create_body["relationship"]["label"], "test_get_rel");

    // Get relationship by GUID and verify content
    let get_response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, rel_guid
        ))
        .send()
        .await
        .expect("Failed to get relationship");

    assert_eq!(get_response.status(), 200);

    let get_body: Value = get_response.json().await.expect("Failed to parse get JSON");
    assert_eq!(get_body["relationship"]["guid"], rel_guid);
    assert_eq!(
        get_body["relationship"]["typeName"],
        "rel_test_table_columns"
    );
    assert_eq!(get_body["relationship"]["label"], "test_get_rel");
}

#[tokio::test]
async fn test_update_relationship() {
    let client = client();

    // Setup
    ensure_entity_types(&client).await;
    ensure_relationship_type(&client).await;
    let (table_guid, column_guid) = create_test_entities(&client).await;

    // Create relationship
    let relationship = json!({
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "original_label"
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await
        .expect("Failed to create relationship");

    let create_body: Value = create_response
        .json()
        .await
        .expect("Failed to parse create JSON");
    let rel_guid = create_body["relationship"]["guid"]
        .as_str()
        .expect("Missing relationship GUID");

    // Update relationship
    let updated_relationship = json!({
        "guid": rel_guid,
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "updated_label"
    });

    let update_response = client
        .put(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&updated_relationship)
        .send()
        .await
        .expect("Failed to update relationship");

    assert_eq!(update_response.status(), 200);

    let update_body: Value = update_response
        .json()
        .await
        .expect("Failed to parse update JSON");
    assert_eq!(update_body["relationship"]["label"], "updated_label");
}

#[tokio::test]
async fn test_delete_relationship() {
    let client = client();

    // Setup
    ensure_entity_types(&client).await;
    ensure_relationship_type(&client).await;
    let (table_guid, column_guid) = create_test_entities(&client).await;

    // Create relationship
    let relationship = json!({
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "to_be_deleted"
    });

    let create_response = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await
        .expect("Failed to create relationship");

    let create_body: Value = create_response
        .json()
        .await
        .expect("Failed to parse create JSON");
    let rel_guid = create_body["relationship"]["guid"]
        .as_str()
        .expect("Missing relationship GUID");

    // Delete relationship
    let delete_response = client
        .delete(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, rel_guid
        ))
        .send()
        .await
        .expect("Failed to delete relationship");

    assert_eq!(delete_response.status(), 204);

    // Verify it's deleted - should return 404
    let get_response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, rel_guid
        ))
        .send()
        .await
        .expect("Failed to get relationship");

    assert_eq!(get_response.status(), 404);
}

#[tokio::test]
async fn test_recreate_relationship_after_deletion() {
    let client = client();

    // Setup
    ensure_entity_types(&client).await;
    ensure_relationship_type(&client).await;
    let (table_guid, column_guid) = create_test_entities(&client).await;

    let relationship = json!({
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "test_recreate"
    });

    // 1. Create relationship
    let create_response1 = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await
        .expect("Failed to create relationship");

    let status = create_response1.status();
    assert!(
        status == 201 || status == 200,
        "First creation should succeed, got {}",
        status
    );

    let create_body1: Value = create_response1.json().await.expect("Failed to parse JSON");
    let guid1 = create_body1["relationship"]["guid"]
        .as_str()
        .expect("GUID should be present");

    // Verify creation
    let get_response1 = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, guid1
        ))
        .send()
        .await
        .expect("Failed to get relationship");
    assert_eq!(get_response1.status(), 200);

    // 2. Delete relationship
    let delete_response = client
        .delete(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, guid1
        ))
        .send()
        .await
        .expect("Failed to delete relationship");
    assert_eq!(delete_response.status(), 204);

    // Verify deletion
    let get_response2 = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, guid1
        ))
        .send()
        .await
        .expect("Failed to get relationship");
    assert_eq!(
        get_response2.status(),
        404,
        "Relationship should be deleted"
    );

    // 3. Recreate relationship (will get a new GUID)
    let create_response2 = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await
        .expect("Failed to recreate relationship");

    let status = create_response2.status();
    assert!(
        status == 201 || status == 200,
        "Recreation after deletion should succeed, got {}",
        status
    );

    let create_body2: Value = create_response2.json().await.expect("Failed to parse JSON");
    let guid2 = create_body2["relationship"]["guid"]
        .as_str()
        .expect("GUID should be present");

    // Verify recreation - new GUID should be different
    assert_ne!(
        guid1, guid2,
        "New relationship should have a different GUID"
    );

    let get_response3 = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, guid2
        ))
        .send()
        .await
        .expect("Failed to get recreated relationship");
    assert_eq!(get_response3.status(), 200);
    let get_body: Value = get_response3.json().await.expect("Failed to parse JSON");
    assert_eq!(get_body["relationship"]["guid"], guid2);
    assert_eq!(
        get_body["relationship"]["typeName"],
        "rel_test_table_columns"
    );
    assert_eq!(get_body["relationship"]["label"], "test_recreate");

    // Cleanup
    client
        .delete(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, guid2
        ))
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn test_get_nonexistent_relationship() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/guid/nonexistent-guid-12345",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_list_relationships_by_entity() {
    let client = client();

    // Setup
    ensure_entity_types(&client).await;
    ensure_relationship_type(&client).await;
    let (table_guid, column_guid) = create_test_entities(&client).await;

    // Create relationship
    let relationship = json!({
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "list_test"
    });

    let _ = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await;

    // List relationships by table entity
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/entity/{}",
            TEST_SERVER_URL, table_guid
        ))
        .send()
        .await
        .expect("Failed to list relationships");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert!(body.is_array());
    let rels = body.as_array().unwrap();
    assert!(!rels.is_empty(), "Should have at least one relationship");

    // Verify the relationship is found
    let found = rels.iter().any(|r| r["label"] == "list_test");
    assert!(found, "Created relationship should be in the list");
}

#[tokio::test]
async fn test_list_relationships_by_type() {
    let client = client();

    // Setup
    ensure_entity_types(&client).await;
    ensure_relationship_type(&client).await;
    let (table_guid, column_guid) = create_test_entities(&client).await;

    // Create relationship
    let relationship = json!({
        "typeName": "rel_test_table_columns",
        "end1": {
            "typeName": "RelTestTable",
            "guid": table_guid
        },
        "end2": {
            "typeName": "RelTestColumn",
            "guid": column_guid
        },
        "label": "type_list_test"
    });

    let _ = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await;

    // List relationships by type
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/type/rel_test_table_columns",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to list relationships by type");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert!(body.is_array());
    let rels = body.as_array().unwrap();
    assert!(!rels.is_empty(), "Should have at least one relationship");

    // All relationships should be of the correct type
    for rel in rels {
        assert_eq!(rel["typeName"], "rel_test_table_columns");
    }
}

#[tokio::test]
async fn test_list_relationships_by_nonexistent_type() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/type/nonexistent_type_xyz",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to list relationships");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert!(body.is_array());
    assert!(body.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_relationships_by_nonexistent_entity() {
    let client = client();

    let response = client
        .get(format!(
            "{}/api/metavisor/v1/relationship/entity/nonexistent-guid-xyz",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to list relationships");

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert!(body.is_array());
    assert!(body.as_array().unwrap().is_empty());
}
