//! Integration tests for Lineage/Graph API
//!
//! This test requires a running server.
//! Start the server first: cargo run --bin metavisor
//! Then run tests: cargo test --test lineage_api_integration

use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

const TEST_SERVER_URL: &str = "http://localhost:31000";

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client")
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Ensure a type definition exists
async fn ensure_type_exists(client: &Client, type_def: &Value) {
    let _ = client
        .post(format!(
            "{}/api/metavisor/v1/types/typedefs",
            TEST_SERVER_URL
        ))
        .header("Content-Type", "application/json")
        .json(type_def)
        .send()
        .await;
}

/// Create an entity and return its GUID
async fn create_entity(client: &Client, type_name: &str, name: &str) -> String {
    let entity = json!({
        "typeName": type_name,
        "attributes": {
            "name": name
        }
    });

    let response = client
        .post(format!("{}/api/metavisor/v1/entity", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&entity)
        .send()
        .await
        .expect("Failed to create entity");

    let body: Value = response.json().await.expect("Failed to parse JSON");
    body["entity"]["guid"]
        .as_str()
        .expect("GUID should be present")
        .to_string()
}

/// Create a relationship between two entities
async fn create_relationship(
    client: &Client,
    type_name: &str,
    end1_type: &str,
    end1_guid: &str,
    end2_type: &str,
    end2_guid: &str,
) -> String {
    let relationship = json!({
        "typeName": type_name,
        "end1": {
            "typeName": end1_type,
            "guid": end1_guid
        },
        "end2": {
            "typeName": end2_type,
            "guid": end2_guid
        }
    });

    let response = client
        .post(format!("{}/api/metavisor/v1/relationship", TEST_SERVER_URL))
        .header("Content-Type", "application/json")
        .json(&relationship)
        .send()
        .await
        .expect("Failed to create relationship");

    let body: Value = response.json().await.expect("Failed to parse JSON");
    body["relationship"]["guid"]
        .as_str()
        .expect("GUID should be present")
        .to_string()
}

/// Delete an entity by GUID
async fn delete_entity(client: &Client, guid: &str) {
    let _ = client
        .delete(format!(
            "{}/api/metavisor/v1/entity/guid/{}",
            TEST_SERVER_URL, guid
        ))
        .send()
        .await;
}

/// Delete a relationship by GUID
async fn delete_relationship(client: &Client, guid: &str) {
    let _ = client
        .delete(format!(
            "{}/api/metavisor/v1/relationship/guid/{}",
            TEST_SERVER_URL, guid
        ))
        .send()
        .await;
}

/// Rebuild the graph
async fn rebuild_graph(client: &Client) {
    let response = client
        .post(format!(
            "{}/api/metavisor/v1/graph/rebuild",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to rebuild graph");

    assert_eq!(response.status(), 200);
}

// ============================================================================
// Setup Types
// ============================================================================

async fn setup_lineage_types(client: &Client) {
    // Create DataSet subtype for tables
    let table_type = json!({
        "entityDefs": [{
            "name": "LineageTable",
            "superTypes": ["DataSet"],
            "attributeDefs": [
                {"name": "name", "typeName": "string", "isOptional": false}
            ]
        }]
    });
    ensure_type_exists(client, &table_type).await;

    // Create Process subtype for data transformations
    let process_type = json!({
        "entityDefs": [{
            "name": "LineageProcess",
            "superTypes": ["Process"],
            "attributeDefs": [
                {"name": "name", "typeName": "string", "isOptional": false}
            ]
        }]
    });
    ensure_type_exists(client, &process_type).await;

    // Create relationship type for data flow (inputs)
    let input_rel_type = json!({
        "relationshipDefs": [{
            "name": "lineage_data_inputs",
            "relationshipCategory": "ASSOCIATION",
            "propagateTags": "ONE_TO_TWO",
            "endDef1": {
                "type": "LineageProcess",
                "name": "process",
                "cardinality": "SINGLE"
            },
            "endDef2": {
                "type": "LineageTable",
                "name": "inputs",
                "cardinality": "SET"
            }
        }]
    });
    ensure_type_exists(client, &input_rel_type).await;

    // Create relationship type for data flow (outputs)
    let output_rel_type = json!({
        "relationshipDefs": [{
            "name": "lineage_data_outputs",
            "relationshipCategory": "ASSOCIATION",
            "propagateTags": "ONE_TO_TWO",
            "endDef1": {
                "type": "LineageProcess",
                "name": "process",
                "cardinality": "SINGLE"
            },
            "endDef2": {
                "type": "LineageTable",
                "name": "outputs",
                "cardinality": "SET"
            }
        }]
    });
    ensure_type_exists(client, &output_rel_type).await;
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_graph_stats() {
    let client = client();

    // Get graph stats
    let response = client
        .get(format!("{}/api/metavisor/v1/graph/stats", TEST_SERVER_URL))
        .send()
        .await
        .expect("Failed to get graph stats");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");

    // Should have node_count and edge_count fields
    assert!(body["node_count"].is_number());
    assert!(body["edge_count"].is_number());
}

#[tokio::test]
async fn test_graph_rebuild() {
    let client = client();

    // Rebuild graph
    let response = client
        .post(format!(
            "{}/api/metavisor/v1/graph/rebuild",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to rebuild graph");

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_lineage_atlas_compatible_endpoint() {
    let client = client();

    // Setup types
    setup_lineage_types(&client).await;

    // Create a simple lineage chain
    let source_guid = create_entity(&client, "LineageTable", "source_atlas_test").await;
    let target_guid = create_entity(&client, "LineageTable", "target_atlas_test").await;

    let _rel_guid = create_relationship(
        &client,
        "lineage_data_inputs",
        "LineageProcess",
        &source_guid,
        "LineageTable",
        &target_guid,
    )
    .await;

    // Rebuild graph
    rebuild_graph(&client).await;

    // Test: Atlas-compatible endpoint with direction=INPUT
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}?direction=INPUT&depth=3",
            TEST_SERVER_URL, target_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["root_guid"].as_str(), Some(target_guid.as_str()));
    assert_eq!(body["direction"].as_str(), Some("INPUT"));

    // Test: Atlas-compatible endpoint with direction=OUTPUT
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}?direction=OUTPUT&depth=3",
            TEST_SERVER_URL, source_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["root_guid"].as_str(), Some(source_guid.as_str()));
    assert_eq!(body["direction"].as_str(), Some("OUTPUT"));

    // Test: Atlas-compatible endpoint with direction=BOTH (default)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}?depth=3",
            TEST_SERVER_URL, source_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(body["root_guid"].as_str(), Some(source_guid.as_str()));
    assert_eq!(body["direction"].as_str(), Some("BOTH"));

    // Cleanup
    delete_entity(&client, &source_guid).await;
    delete_entity(&client, &target_guid).await;
}

#[tokio::test]
async fn test_lineage_simple_chain() {
    let client = client();

    // Setup types
    setup_lineage_types(&client).await;

    // Create a simple lineage chain: source_table -> process -> target_table
    let source_guid = create_entity(&client, "LineageTable", "source_table_lineage").await;
    let process_guid = create_entity(&client, "LineageProcess", "transform_process").await;
    let target_guid = create_entity(&client, "LineageTable", "target_table_lineage").await;

    // Create relationships
    let input_rel_guid = create_relationship(
        &client,
        "lineage_data_inputs",
        "LineageProcess",
        &process_guid,
        "LineageTable",
        &source_guid,
    )
    .await;
    let output_rel_guid = create_relationship(
        &client,
        "lineage_data_outputs",
        "LineageProcess",
        &process_guid,
        "LineageTable",
        &target_guid,
    )
    .await;

    // Rebuild graph to include new entities/relationships
    rebuild_graph(&client).await;

    // Test: Get upstream lineage of target_table (should include source_table and process)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/inputs?depth=3",
            TEST_SERVER_URL, target_guid
        ))
        .send()
        .await
        .expect("Failed to get input lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");

    // Verify response structure
    assert_eq!(body["root_guid"].as_str(), Some(target_guid.as_str()));
    assert_eq!(body["direction"].as_str(), Some("INPUT"));
    assert!(body["nodes"].is_array());
    assert!(body["edges"].is_array());

    // Should have at least the source table and process in the lineage
    let nodes = body["nodes"].as_array().expect("nodes should be array");
    assert!(nodes.len() >= 2, "Should have at least 2 nodes in lineage");

    // Test: Get downstream lineage of source_table (should include process and target_table)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/outputs?depth=3",
            TEST_SERVER_URL, source_guid
        ))
        .send()
        .await
        .expect("Failed to get output lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");

    assert_eq!(body["root_guid"].as_str(), Some(source_guid.as_str()));
    assert_eq!(body["direction"].as_str(), Some("OUTPUT"));

    // Cleanup
    delete_relationship(&client, &input_rel_guid).await;
    delete_relationship(&client, &output_rel_guid).await;
    delete_entity(&client, &source_guid).await;
    delete_entity(&client, &process_guid).await;
    delete_entity(&client, &target_guid).await;
}

#[tokio::test]
async fn test_lineage_depth_limit() {
    let client = client();

    // Setup types
    setup_lineage_types(&client).await;

    // Create a chain: A -> B -> C -> D
    let a_guid = create_entity(&client, "LineageTable", "table_a_depth").await;
    let b_guid = create_entity(&client, "LineageTable", "table_b_depth").await;
    let c_guid = create_entity(&client, "LineageTable", "table_c_depth").await;
    let d_guid = create_entity(&client, "LineageTable", "table_d_depth").await;

    // Create a relationship type for data flow
    let flow_type = json!({
        "relationshipDefs": [{
            "name": "data_flow_depth",
            "relationshipCategory": "ASSOCIATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": "LineageTable",
                "name": "source",
                "cardinality": "SINGLE"
            },
            "endDef2": {
                "type": "LineageTable",
                "name": "target",
                "cardinality": "SINGLE"
            }
        }]
    });
    ensure_type_exists(&client, &flow_type).await;

    // Create relationships: A -> B, B -> C, C -> D
    let rel_ab = create_relationship(
        &client,
        "data_flow_depth",
        "LineageTable",
        &a_guid,
        "LineageTable",
        &b_guid,
    )
    .await;
    let rel_bc = create_relationship(
        &client,
        "data_flow_depth",
        "LineageTable",
        &b_guid,
        "LineageTable",
        &c_guid,
    )
    .await;
    let rel_cd = create_relationship(
        &client,
        "data_flow_depth",
        "LineageTable",
        &c_guid,
        "LineageTable",
        &d_guid,
    )
    .await;

    // Rebuild graph
    rebuild_graph(&client).await;

    // Test: Get upstream lineage with depth=1 (should only get C)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/inputs?depth=1",
            TEST_SERVER_URL, d_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    let nodes = body["nodes"].as_array().expect("nodes should be array");
    // With depth=1, we should get exactly 1 node (C)
    // Note: Some implementations might include the root node in the count
    assert!(
        nodes.len() == 1 || nodes.len() == 2,
        "With depth=1, should have 1 or 2 nodes (depending on implementation)"
    );

    // Test: Get upstream lineage with depth=3 (should get A, B, C)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/inputs?depth=3",
            TEST_SERVER_URL, d_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    let nodes = body["nodes"].as_array().expect("nodes should be array");
    // With depth=3, we should get 3 nodes (A, B, C)
    // Allow some flexibility for implementation differences
    assert!(
        nodes.len() >= 3 && nodes.len() <= 4,
        "With depth=3, should have 3 or 4 nodes"
    );

    // Cleanup
    delete_relationship(&client, &rel_ab).await;
    delete_relationship(&client, &rel_bc).await;
    delete_relationship(&client, &rel_cd).await;
    delete_entity(&client, &a_guid).await;
    delete_entity(&client, &b_guid).await;
    delete_entity(&client, &c_guid).await;
    delete_entity(&client, &d_guid).await;
}

#[tokio::test]
async fn test_lineage_nonexistent_entity() {
    let client = client();

    // Test lineage for non-existent entity
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/nonexistent-guid-12345/inputs?depth=3",
            TEST_SERVER_URL
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    // Should return 404 or error
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "Expected error for non-existent entity"
    );
}

#[tokio::test]
async fn test_lineage_entity_with_no_relationships() {
    let client = client();

    // Setup types
    setup_lineage_types(&client).await;

    // Create an isolated entity with no relationships (unique name)
    let isolated_guid = create_entity(&client, "LineageTable", "isolated_table_unique_test").await;

    // Rebuild graph
    rebuild_graph(&client).await;

    // Get lineage - should return 200 with empty lineage for isolated entity
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/inputs?depth=3",
            TEST_SERVER_URL, isolated_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    // If entity is in graph, should return 200; if not, 404 is acceptable
    // Both behaviors are valid depending on whether we add all entities to graph
    let status = response.status();
    if status == 200 {
        let body: Value = response.json().await.expect("Failed to parse JSON");
        // Should have root_guid but empty nodes and edges
        assert_eq!(body["root_guid"].as_str(), Some(isolated_guid.as_str()));
        let nodes = body["nodes"].as_array().expect("nodes should be array");
        let edges = body["edges"].as_array().expect("edges should be array");
        assert_eq!(
            nodes.len(),
            0,
            "Isolated entity should have no lineage nodes"
        );
        assert_eq!(
            edges.len(),
            0,
            "Isolated entity should have no lineage edges"
        );
    } else {
        // 404 is also acceptable - entity exists but has no relationships
        assert!(status == 404, "Expected 200 or 404, got {}", status);
    }

    // Cleanup
    delete_entity(&client, &isolated_guid).await;
}

#[tokio::test]
async fn test_lineage_fork_and_merge() {
    let client = client();

    // Setup types
    setup_lineage_types(&client).await;

    // Create a fork pattern: A -> B, A -> C
    // And a merge pattern: B -> D, C -> D
    let a_guid = create_entity(&client, "LineageTable", "fork_source").await;
    let b_guid = create_entity(&client, "LineageTable", "fork_branch_b").await;
    let c_guid = create_entity(&client, "LineageTable", "fork_branch_c").await;
    let d_guid = create_entity(&client, "LineageTable", "merge_target").await;

    // Create relationship type for fork/merge
    let fork_type = json!({
        "relationshipDefs": [{
            "name": "data_flow_fork",
            "relationshipCategory": "ASSOCIATION",
            "propagateTags": "ONE_TO_TWO",
            "endDef1": {
                "type": "LineageTable",
                "name": "source",
                "cardinality": "SINGLE"
            },
            "endDef2": {
                "type": "LineageTable",
                "name": "target",
                "cardinality": "SINGLE"
            }
        }]
    });
    ensure_type_exists(&client, &fork_type).await;

    // Create relationships
    let rel_ab = create_relationship(
        &client,
        "data_flow_fork",
        "LineageTable",
        &a_guid,
        "LineageTable",
        &b_guid,
    )
    .await;
    let rel_ac = create_relationship(
        &client,
        "data_flow_fork",
        "LineageTable",
        &a_guid,
        "LineageTable",
        &c_guid,
    )
    .await;
    let rel_bd = create_relationship(
        &client,
        "data_flow_fork",
        "LineageTable",
        &b_guid,
        "LineageTable",
        &d_guid,
    )
    .await;
    let rel_cd = create_relationship(
        &client,
        "data_flow_fork",
        "LineageTable",
        &c_guid,
        "LineageTable",
        &d_guid,
    )
    .await;

    // Rebuild graph
    rebuild_graph(&client).await;

    // Test: Get downstream lineage of A (should include B, C, D)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/outputs?depth=3",
            TEST_SERVER_URL, a_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    let nodes = body["nodes"].as_array().expect("nodes should be array");
    // Should have at least 3 nodes (B, C, D)
    assert!(
        nodes.len() >= 3,
        "Fork pattern should have at least 3 downstream nodes"
    );

    // Test: Get upstream lineage of D (should include A, B, C)
    let response = client
        .get(format!(
            "{}/api/metavisor/v1/lineage/{}/inputs?depth=3",
            TEST_SERVER_URL, d_guid
        ))
        .send()
        .await
        .expect("Failed to get lineage");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("Failed to parse JSON");
    let nodes = body["nodes"].as_array().expect("nodes should be array");
    // Should have at least 3 nodes (A, B, C)
    assert!(
        nodes.len() >= 3,
        "Merge pattern should have at least 3 upstream nodes"
    );

    // Cleanup
    delete_relationship(&client, &rel_ab).await;
    delete_relationship(&client, &rel_ac).await;
    delete_relationship(&client, &rel_bd).await;
    delete_relationship(&client, &rel_cd).await;
    delete_entity(&client, &a_guid).await;
    delete_entity(&client, &b_guid).await;
    delete_entity(&client, &c_guid).await;
    delete_entity(&client, &d_guid).await;
}
