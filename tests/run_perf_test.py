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
    """Generate payload for creating an entity."""
    suffix = f"{prefix}_{index}" if prefix else str(index)
    return {
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


def build_create_relationship_payload(index: int) -> dict[str, Any]:
    """Generate payload for creating a relationship using existing entities.
    
    Uses pre-existing entities from prepare_data to avoid concurrency issues.
    Alternates between two column entities to distribute load.
    """
    # Alternate between the two existing columns
    column_qn = "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID" if index % 2 == 0 else "BDSP_SPCP.T80_PC8_CPS_ASSET_AVG_Y.CUST_ID"
    return {
        "typeName": "join_relationship",
        "attributes": {
            "rel_type": "test_join",
            "expression": f"test_expr_{index}",
        },
        "end1": {
            "typeName": "column_meta",
            "uniqueAttributes": {"qualifiedName": column_qn}
        },
        "end2": {
            "typeName": "column_meta",
            "uniqueAttributes": {"qualifiedName": "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID" if index % 2 == 1 else "BDSP_SPCP.T80_PC8_CPS_ASSET_AVG_Y.CUST_ID"}
        }
    }


def build_workload(
    total_requests: int, 
    relationship_guid: str | None = None
) -> list[tuple[str, str, dict[str, Any] | None]]:
    """Build mixed read/write workload.
    
    Read operations (40%):
        - GET entity by unique attribute
        - GET type definition
        - GET relationship by GUID
    
    Write operations (60%):
        - POST create type
        - POST create entity  
        - POST create relationship
        - POST update entity
    
    Note: To avoid conflicts in concurrent environment, delete operations
    target pre-existing entities from prepare_data phase, not newly created ones.
    """
    qn_1 = "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID"
    qn_2 = "BDSP_SPCP.T80_PC8_CPS_ASSET_AVG_Y.CUST_ID"
    sql_qn = "397dd24490e1020c8cc869276d1be0c5"

    operations: list[tuple[str, str, dict[str, Any] | None]] = []
    import uuid
    base_uuid = uuid.uuid4().hex[:8]
    
    for i in range(total_requests):
        op_type = i % 11
        if op_type == 0:
            # Read: Get entity 1
            operations.append(("GET", f"/entity/uniqueAttribute/type/column_meta?attr:qualifiedName={qn_1}", None))
        elif op_type == 1:
            # Read: Get entity 2
            operations.append(("GET", f"/entity/uniqueAttribute/type/column_meta?attr:qualifiedName={qn_2}", None))
        elif op_type == 2:
            # Read: Get SQL meta
            operations.append(("GET", f"/entity/uniqueAttribute/type/sql_meta?attr:qualifiedName={sql_qn}", None))
        elif op_type == 3:
            # Read: Get type definition
            operations.append(("GET", f"/types/typedef/name/column_meta", None))
        elif op_type == 4:
            # Read: Get relationship by GUID (skip if no GUID available)
            if relationship_guid:
                operations.append(("GET", f"/relationship/guid/{relationship_guid}", None))
            else:
                # Fallback to type definition read
                operations.append(("GET", f"/types/typedef/name/sql_meta", None))
        elif op_type == 5:
            # Write: Create type (use unique prefix to avoid conflicts)
            operations.append(("POST", f"/types/typedefs", build_create_type_payload(i, base_uuid)))
        elif op_type == 6:
            # Write: Create entity
            operations.append(("POST", f"/entity", build_create_entity_payload(i, base_uuid)))
        elif op_type == 7:
            # Write: Create relationship
            operations.append(("POST", f"/relationship", build_create_relationship_payload(i)))
        elif op_type == 8:
            # Write: Update entity (use pre-existing entity to avoid conflicts)
            # Note: Update requires all mandatory attributes to be present
            operations.append(("POST", f"/entity", {
                "typeName": "column_meta",
                "attributes": {
                    "qualifiedName": qn_1,
                    "column_id": qn_1,
                    "name": "PARTY_ID",
                    "column_name": "PARTY_ID",
                    "table_name": "T80_PC8_CPS_PBK",
                    "db_name": "BDSP_SPCP",
                    "table_id": "110000000520363",
                    "column_description_short": f"Updated description {i}",
                }
            }))
        elif op_type == 9:
            # Read: Get another type
            operations.append(("GET", "/types/typedef/name/sql_meta", None))
        else:
            # Write: Create another entity (high write ratio, use different prefix)
            operations.append(("POST", "/entity", build_create_entity_payload(i, f"{base_uuid}_extra")))

    return operations


def prepare_data(base_url: str) -> str | None:
    log_section("Preparing Dataset")
    # Extract root URL (without API prefix) for run_test_data
    # e.g., "http://localhost:31000/api/metavisor/v1" -> "http://localhost:31000"
    import re
    root_url = re.sub(r'/api/.*', '', base_url)
    if not root_url:
        root_url = base_url
    client = MetavisorClient(root_url)
    runner = TestRunner(client)
    runner.cleanup()
    runner.create_types()
    runner.create_entities()
    runner.create_relationships()
    runner.create_lineage_relationships()
    
    # Get a relationship GUID for read testing
    relationship_guid = runner.relationship_guids[0] if runner.relationship_guids else None
    if relationship_guid:
        log_info(f"Using relationship GUID for read testing: {relationship_guid}")
    
    log_success("Dataset ready for performance testing")
    return relationship_guid


def execute_request(args: tuple[str, str, str, dict[str, Any] | None]) -> PerfResult:
    """Execute a single HTTP request. Used as worker function for ProcessPoolExecutor.

    Each process creates its own session to avoid GIL contention and connection sharing issues.
    """
    base_url, method, path, json_body = args
    started = time.perf_counter()
    try:
        # Create a new session per request (ProcessPool runs in separate processes)
        with requests.Session() as session:
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


def run_benchmark(
    base_url: str,
    total_requests: int,
    concurrency: int,
    relationship_guid: str | None = None
) -> list[PerfResult]:
    log_section("Running Benchmark")
    workload = build_workload(total_requests, relationship_guid)

    # Prepare arguments for ProcessPoolExecutor (base_url + operation details)
    tasks = [(base_url, method, path, body) for method, path, body in workload]

    started = time.perf_counter()
    # Use fixed chunksize for better performance (avoid too small chunks)
    chunksize = max(16, len(tasks) // concurrency)
    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(execute_request, tasks, chunksize=chunksize))
    elapsed_s = time.perf_counter() - started

    success = sum(1 for result in results if result.ok)
    failed = len(results) - success
    durations = sorted(result.duration_ms for result in results)

    print(f"Total requests: {len(results)}")
    print(f"Concurrency: {concurrency}")
    print(f"Elapsed: {elapsed_s:.2f}s")
    print(f"Throughput: {len(results) / elapsed_s:.2f} req/s")
    print(f"Success: {success}")
    print(f"Failed: {failed}")
    print(f"Avg latency: {statistics.fmean(durations):.2f} ms")
    print(f"P50 latency: {percentile(durations, 0.50):.2f} ms")
    print(f"P95 latency: {percentile(durations, 0.95):.2f} ms")
    print(f"P99 latency: {percentile(durations, 0.99):.2f} ms")

    if failed:
        log_error("Some requests failed. Sample failures:")
        for result in [r for r in results if not r.ok][:5]:
            print(f"- {result.name}: status={result.status_code} error={result.error}")

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Metavisor API performance test")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL with path prefix (e.g., http://localhost:31000/api/metavisor/v1 or http://8.92.9.185:21000/api/atlas/v2)")
    parser.add_argument("--requests", type=int, default=200, help="Total requests to execute")
    parser.add_argument("--concurrency", type=int, default=16, help="Concurrent worker count")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    client = MetavisorClient(args.base_url)
    if not client.check_server():
        return 1

    relationship_guid = prepare_data(args.base_url)
    results = run_benchmark(args.base_url, args.requests, args.concurrency, relationship_guid)

    if any(not result.ok for result in results):
        return 1

    log_success("Performance test completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
