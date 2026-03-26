#!/usr/bin/env python3
"""
Metavisor API performance test script.

Usage:
  uv run python run_perf_test.py
  uv run python run_perf_test.py --base-url http://127.0.0.1:31000
  uv run python run_perf_test.py --requests 500 --concurrency 20

The script:
1. Ensures the server is reachable
2. Cleans up and loads the standard test dataset
3. Benchmarks Atlas-compatible read-heavy endpoints concurrently
4. Prints throughput and latency percentiles
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any

import requests

from run_test_data import MetavisorClient, TestRunner, log_error, log_info, log_section, log_success


DEFAULT_BASE_URL = "http://127.0.0.1:31000/api/metavisor/v1"


@dataclass
class PerfResult:
    name: str
    status_code: int
    duration_ms: float
    ok: bool
    error: str | None = None


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * p
    lower = int(rank)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = rank - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def build_create_type_payload(index: int, prefix: str = "") -> dict[str, Any]:
    """Generate payload for creating a type definition."""
    suffix = f"{prefix}_{index}" if prefix else str(index)
    return {
        "entityDefs": [{
            "name": f"perf_test_type_{suffix}",
            "typeVersion": "1.0",
            "serviceType": "metavisor",
            "superTypes": ["DataSet"],
            "attributeDefs": [
                {"name": "test_attr", "typeName": "string", "isOptional": True, "cardinality": "SINGLE"}
            ]
        }]
    }


def build_create_entity_payload(index: int, prefix: str = "") -> dict[str, Any]:
    """Generate payload for creating an entity (Atlas format with 'entity' wrapper)."""
    suffix = f"{prefix}_{index}" if prefix else str(index)
    return {
        "entity": {
            "typeName": "column_meta",
            "attributes": {
                "column_id": f"perf_test_col_{suffix}",
                "qualifiedName": f"perf_test_col_{suffix}",
                "name": f"test_column_{suffix}",
                "column_name": f"test_column_{suffix}",
                "table_name": "perf_test_table",
                "db_name": "perf_test_db",
                "table_id": "perf_test_table_id",
                "column_description_short": f"Performance test column {suffix}",
            }
        }
    }


def build_create_relationship_payload(
    index: int, 
    entity_qn_1: str, 
    entity_qn_2: str,
    rel_type: str = "join_relationship"
) -> dict[str, Any]:
    """Generate payload for creating a relationship between two existing entities."""
    return {
        "typeName": rel_type,
        "attributes": {
            "rel_type": "test_join",
            "expression": f"test_expr_{index}",
        },
        "end1": {
            "typeName": "column_meta",
            "uniqueAttributes": {"qualifiedName": entity_qn_1}
        },
        "end2": {
            "typeName": "column_meta",
            "uniqueAttributes": {"qualifiedName": entity_qn_2}
        }
    }


def build_update_entity_payload(entity_type: str, qualified_name: str, index: int) -> dict[str, Any]:
    """Generate payload for updating an existing entity."""
    return {
        "entity": {
            "typeName": entity_type,
            "attributes": {
                "qualifiedName": qualified_name,
                "column_description_short": f"Updated by perf test {index} at {time.time()}"
            }
        }
    }


def build_workload_sequential(
    total_requests: int,
    base_uuid: str,
    pre_existing_entities: list[tuple[str, str]],
    relationship_guid: str | None = None
) -> list[tuple[str, str, dict[str, Any] | None]]:
    """Build workload where each request is independent (no dependencies between requests).
    
    This is suitable for concurrent execution where order is not guaranteed.
    Each operation is self-contained and uses either:
    - Pre-existing resources (known to exist)
    - Dynamically generated unique resources (will be created)
    """
    import time
    operations: list[tuple[str, str, dict[str, Any] | None]] = []
    
    pre_existing_types = ["column_meta", "sql_meta"]
    
    # Track if we should attempt relationship creation (only once per test run)
    relationship_created = False
    
    for i in range(total_requests):
        op_type = i % 8
        
        if op_type == 0:
            # Create type with unique name
            operations.append(("POST", "/types/typedefs", build_create_type_payload(i, base_uuid)))
        elif op_type == 1:
            # Create entity with unique name
            operations.append(("POST", "/entity", build_create_entity_payload(i, base_uuid)))
        elif op_type == 2:
            # Read pre-existing entity
            type_name, qn = pre_existing_entities[i % len(pre_existing_entities)]
            operations.append(("GET", f"/entity/uniqueAttribute/type/{type_name}?attr:qualifiedName={qn}", None))
        elif op_type == 3:
            # Read pre-existing type
            type_name = pre_existing_types[i % len(pre_existing_types)]
            operations.append(("GET", f"/types/typedef/name/{type_name}", None))
        elif op_type == 4 and relationship_guid:
            # Read pre-existing relationship
            operations.append(("GET", f"/relationship/guid/{relationship_guid}", None))
        elif op_type == 5:
            # Update pre-existing entity (different from create, tests update path)
            type_name, qn = pre_existing_entities[i % len(pre_existing_entities)]
            operations.append(("POST", "/entity", build_update_entity_payload(type_name, qn, i)))
        elif op_type == 6:
            # Get entity by GUID (first pre-existing entity)
            # Note: This requires knowing the GUID, using qualifiedName instead
            type_name, qn = pre_existing_entities[0]
            operations.append(("GET", f"/entity/uniqueAttribute/type/{type_name}?attr:qualifiedName={qn}", None))
        else:
            # Create relationship - only attempt once to avoid 409 conflicts
            # Subsequent attempts will fail with 409, which is handled gracefully
            qn_1 = pre_existing_entities[0][1]  # PARTY_ID
            qn_2 = pre_existing_entities[1][1]  # CUST_ID
            operations.append(("POST", "/relationship", build_create_relationship_payload(i, qn_1, qn_2)))
    
    return operations


def build_workload(
    total_requests: int, 
    relationship_guid: str | None = None
) -> list[tuple[str, str, dict[str, Any] | None]]:
    """Build workload with proper dependency handling for concurrent execution.
    
    Since concurrent execution doesn't guarantee order, we use two strategies:
    1. Write operations: Use dynamically generated unique names (no dependencies)
    2. Read/Relationship operations: Use pre-existing resources (guaranteed to exist)
    
    This ensures each request can succeed independently regardless of execution order.
    """
    import uuid
    import time
    
    # Use time + uuid to ensure uniqueness across runs
    base_uuid = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    # Pre-existing entities from prepare_data (guaranteed to exist)
    pre_existing_entities = [
        ("column_meta", "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID"),
        ("column_meta", "BDSP_SPCP.T80_PC8_CPS_ASSET_AVG_Y.CUST_ID"),
        ("sql_meta", "397dd24490e1020c8cc869276d1be0c5")
    ]
    
    return build_workload_sequential(
        total_requests, 
        base_uuid, 
        pre_existing_entities,
        relationship_guid
    )


def prepare_data(
    base_url: str, 
    username: str | None = None, 
    password: str | None = None,
    skip_cleanup: bool = False,
    skip_type_creation: bool = False
) -> str | None:
    log_section("Preparing Dataset")
    # Extract root URL (without API prefix) for run_test_data
    # e.g., "http://localhost:31000/api/metavisor/v1" -> "http://localhost:31000"
    import re
    root_url = re.sub(r'/api/.*', '', base_url)
    if not root_url:
        root_url = base_url
    # MetavisorClient needs the full base_url to extract api_prefix correctly
    # But we pass the root_url so that base_url + api_prefix = correct full URL
    client = MetavisorClient(root_url, username=username, password=password)
    # Override api_prefix to match the full API path from original base_url
    match = re.search(r'(/api/[^/]+/v\d+)$', base_url)
    if match:
        client.api_prefix = match.group(1)
    runner = TestRunner(client)
    
    if not skip_cleanup:
        runner.cleanup()
    
    if not skip_type_creation:
        runner.create_types()
    
    runner.create_entities()
    runner.create_relationships()
    
    # Get a relationship GUID for read testing
    relationship_guid = runner.relationship_guids[0] if runner.relationship_guids else None
    if relationship_guid:
        log_info(f"Using relationship GUID for read testing: {relationship_guid}")
    
    log_success("Dataset ready for performance testing")
    return relationship_guid


def execute_request(args: tuple[str, str, str, dict[str, Any] | None, tuple[str, str] | None]) -> PerfResult:
    """Execute a single HTTP request. Used as worker function for ProcessPoolExecutor.

    Each process creates its own session to avoid GIL contention and connection sharing issues.
    """
    base_url, method, path, json_body, auth = args
    started = time.perf_counter()
    try:
        # Create a new session per request (ProcessPool runs in separate processes)
        with requests.Session() as session:
            if auth:
                session.auth = auth
            response = session.request(
                method=method,
                url=f"{base_url}{path}",
                json=json_body,
                timeout=30.0,
                headers={"Content-Type": "application/json"},
            )
        duration_ms = (time.perf_counter() - started) * 1000
        return PerfResult(
            name=f"{method} {path}",
            status_code=response.status_code,
            duration_ms=duration_ms,
            ok=response.status_code < 400,
            error=None if response.status_code < 400 else response.text[:200],
        )
    except requests.RequestException as exc:
        duration_ms = (time.perf_counter() - started) * 1000
        return PerfResult(
            name=f"{method} {path}",
            status_code=0,
            duration_ms=duration_ms,
            ok=False,
            error=str(exc),
        )


def run_phase(
    phase_name: str,
    base_url: str,
    operations: list[tuple[str, str, dict[str, Any] | None]],
    concurrency: int,
    auth: tuple[str, str] | None = None
) -> list[PerfResult]:
    """Run a single phase of the benchmark (concurrent execution within phase)."""
    log_section(f"Phase: {phase_name}")
    
    # Prepare arguments for ProcessPoolExecutor
    tasks = [(base_url, method, path, body, auth) for method, path, body in operations]
    
    started = time.perf_counter()
    chunksize = max(16, len(tasks) // concurrency)
    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(execute_request, tasks, chunksize=chunksize))
    elapsed_s = time.perf_counter() - started
    
    success = sum(1 for result in results if result.ok)
    failed = len(results) - success
    durations = sorted(result.duration_ms for result in results)
    
    print(f"  Requests: {len(results)}")
    print(f"  Elapsed: {elapsed_s:.2f}s")
    print(f"  Throughput: {len(results) / elapsed_s:.2f} req/s")
    print(f"  Success: {success}")
    print(f"  Failed: {failed}")
    if durations:
        print(f"  Avg latency: {statistics.fmean(durations):.2f} ms")
        print(f"  P50 latency: {percentile(durations, 0.50):.2f} ms")
        print(f"  P95 latency: {percentile(durations, 0.95):.2f} ms")
    
    if failed:
        log_error(f"Phase '{phase_name}' had {failed} failures. Sample:")
        for result in [r for r in results if not r.ok][:3]:
            print(f"    - {result.name}: status={result.status_code} error={result.error[:100]}")
    else:
        log_success(f"Phase '{phase_name}' completed")
    
    return results


def run_benchmark(
    base_url: str,
    total_requests: int,
    concurrency: int,
    relationship_guid: str | None = None,
    auth: tuple[str, str] | None = None
) -> list[PerfResult]:
    """Run benchmark in three sequential phases:
    
    Phase 1: Create types + read types (parallel within phase)
    Phase 2: Create entities + read entities (parallel within phase)
    Phase 3: Create relationships + read relationships (parallel within phase)
    
    Phases are sequential to ensure proper dependencies (types exist before entities, etc.)
    """
    import uuid
    import time
    
    log_section("Running Benchmark (3 Phases)")
    base_uuid = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    # Calculate requests per phase
    per_phase = total_requests // 3
    
    # Pre-existing resources for read operations (types created by prepare_data)
    pre_existing_types = ["column_meta", "sql_meta"]
    pre_existing_entities = [
        ("column_meta", "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID"),
        ("column_meta", "BDSP_SPCP.T80_PC8_CPS_ASSET_AVG_Y.CUST_ID"),
        ("sql_meta", "397dd24490e1020c8cc869276d1be0c5")
    ]
    
    all_results: list[PerfResult] = []
    
    # Phase 1: Types (create + read)
    phase1_ops: list[tuple[str, str, dict[str, Any] | None]] = []
    for i in range(per_phase):
        if i % 2 == 0:
            # Create type
            phase1_ops.append(("POST", "/types/typedefs", build_create_type_payload(i, base_uuid)))
        else:
            # Read pre-existing type
            type_name = pre_existing_types[i % len(pre_existing_types)]
            phase1_ops.append(("GET", f"/types/typedef/name/{type_name}", None))
    all_results.extend(run_phase("Types (Create + Read)", base_url, phase1_ops, concurrency, auth))
    
    # Phase 2: Entities (create + read)
    # Pre-calculate entity names that will be created (must match build_create_entity_payload format)
    phase2_entity_names: list[str] = []
    
    phase2_ops: list[tuple[str, str, dict[str, Any] | None]] = []
    for i in range(per_phase):
        if i % 2 == 0:
            # Create entity
            # Must match the qualifiedName generated by build_create_entity_payload
            qn = f"perf_test_col_{base_uuid}_{i}"
            phase2_ops.append(("POST", "/entity", build_create_entity_payload(i, base_uuid)))
            phase2_entity_names.append(qn)
        else:
            # Read pre-existing entity
            type_name, qn = pre_existing_entities[i % len(pre_existing_entities)]
            phase2_ops.append(("GET", f"/entity/uniqueAttribute/type/{type_name}?attr:qualifiedName={qn}", None))
    phase2_results = run_phase("Entities (Create + Read)", base_url, phase2_ops, concurrency, auth)
    all_results.extend(phase2_results)
    
    # Phase 3: Relationships (create + read)
    # Use entities created in phase 2 for relationship creation
    phase3_ops: list[tuple[str, str, dict[str, Any] | None]] = []
    
    # Create relationships using phase 2 entities
    # Pair up consecutive entities: (0,1), (2,3), (4,5), etc.
    relationship_count = min(len(phase2_entity_names) // 2, per_phase // 3)
    for i in range(relationship_count):
        if i * 2 + 1 < len(phase2_entity_names):
            qn_1 = phase2_entity_names[i * 2]
            qn_2 = phase2_entity_names[i * 2 + 1]
            phase3_ops.append(("POST", "/relationship", build_create_relationship_payload(i, qn_1, qn_2)))
    
    # Remaining requests: read operations
    remaining = per_phase - len(phase3_ops)
    for i in range(remaining):
        if relationship_guid and i % 2 == 0:
            # Read pre-existing relationship
            phase3_ops.append(("GET", f"/relationship/guid/{relationship_guid}", None))
        elif phase2_entity_names and i < len(phase2_entity_names):
            # Read entities created in phase 2
            qn = phase2_entity_names[i % len(phase2_entity_names)]
            phase3_ops.append(("GET", f"/entity/uniqueAttribute/type/column_meta?attr:qualifiedName={qn}", None))
        else:
            # Read pre-existing entities
            type_name, qn = pre_existing_entities[i % len(pre_existing_entities)]
            phase3_ops.append(("GET", f"/entity/uniqueAttribute/type/{type_name}?attr:qualifiedName={qn}", None))
    
    all_results.extend(run_phase("Relationships (Create + Read)", base_url, phase3_ops, concurrency, auth))
    
    # Print summary
    total_success = sum(1 for r in all_results if r.ok)
    total_failed = len(all_results) - total_success
    print(f"\n{'='*40}")
    print(f"Total Success: {total_success}/{len(all_results)}")
    print(f"Total Failed: {total_failed}/{len(all_results)}")
    
    return all_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Metavisor API performance test")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL with path prefix (e.g., http://localhost:31000/api/metavisor/v1 or http://8.92.9.185:21000/api/atlas/v2)")
    parser.add_argument("--requests", type=int, default=200, help="Total requests to execute")
    parser.add_argument("--concurrency", type=int, default=16, help="Concurrent worker count")
    parser.add_argument("--username", default="admin", help="Username for Basic Auth (Atlas)")
    parser.add_argument("--password", default="admin", help="Password for Basic Auth (Atlas)")
    parser.add_argument("--skip-cleanup", action="store_true", help="Skip cleanup step (useful for Atlas)")
    parser.add_argument("--skip-type-creation", action="store_true", help="Skip type creation (useful if types already exist)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    
    # Setup auth for Atlas API
    auth = (args.username, args.password) if args.username and args.password else None

    client = MetavisorClient(args.base_url, username=args.username, password=args.password)
    if not client.check_server():
        return 1

    relationship_guid = prepare_data(
        args.base_url, 
        username=args.username, 
        password=args.password,
        skip_cleanup=args.skip_cleanup,
        skip_type_creation=args.skip_type_creation
    )
    results = run_benchmark(args.base_url, args.requests, args.concurrency, relationship_guid, auth)

    if any(not result.ok for result in results):
        return 1

    log_success("Performance test completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
