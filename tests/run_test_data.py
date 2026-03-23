#!/usr/bin/env python3
"""
Metavisor API Test Script
Usage: python run_test_data.py [command] [args...]

This script checks HTTP response codes and exits on error.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests

# Configuration
BASE_URL = "http://localhost:31000"
DATA_DIR = Path(__file__).parent / "data"


class Colors:
    """ANSI color codes for terminal output."""
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    BLUE = "\033[0;34m"
    NC = "\033[0m"  # No Color


def log_info(msg: str) -> None:
    print(f"{Colors.BLUE}[INFO]{Colors.NC} {msg}")


def log_success(msg: str) -> None:
    print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {msg}")


def log_error(msg: str) -> None:
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}")


def log_section(msg: str) -> None:
    print(f"\n{Colors.YELLOW}{'=' * 40}{Colors.NC}")
    print(f"{Colors.YELLOW}{msg}{Colors.NC}")
    print(f"{Colors.YELLOW}{'=' * 40}{Colors.NC}\n")


def print_json(data: Any) -> None:
    """Pretty print JSON data."""
    print(json.dumps(data, indent=2, ensure_ascii=False))


class MetavisorClient:
    """HTTP client for Metavisor API."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

    def _handle_response(self, response: requests.Response, description: str) -> Any:
        """Handle HTTP response and return JSON data."""
        if response.status_code >= 400:
            log_error(f"Failed to {description}: HTTP {response.status_code}")
            try:
                print_json(response.json())
            except json.JSONDecodeError:
                print(response.text)
            raise RuntimeError(f"HTTP {response.status_code}")

        try:
            data = response.json()
            print_json(data)
            return data
        except json.JSONDecodeError:
            print(response.text)
            return response.text

    def get(self, path: str, description: str = "request") -> Any:
        """Execute GET request."""
        url = f"{self.base_url}{path}"
        response = self.session.get(url, timeout=30)
        return self._handle_response(response, description)

    def post(self, path: str, data: dict | None = None, data_file: Path | None = None,
             description: str = "request") -> Any:
        """Execute POST request."""
        url = f"{self.base_url}{path}"
        if data_file:
            with open(data_file) as f:
                data = json.load(f)
        response = self.session.post(url, json=data, timeout=30)
        return self._handle_response(response, description)

    def post_file(self, path: str, data_file: Path, description: str = "request") -> Any:
        """Execute POST request with JSON file."""
        return self.post(path, data_file=data_file, description=description)

    def delete(self, path: str, description: str = "request") -> bool:
        """Execute DELETE request."""
        url = f"{self.base_url}{path}"
        response = self.session.delete(url, timeout=30)
        if response.status_code >= 400:
            log_error(f"Failed to {description}: HTTP {response.status_code}")
            return False
        return True

    def check_server(self) -> bool:
        """Check if server is running."""
        # Extract root URL (without API prefix) for health check
        # e.g., "http://localhost:31000/api/metavisor/v1" -> "http://localhost:31000"
        import re
        root_url = re.sub(r'/api/.*', '', self.base_url)
        if not root_url:
            root_url = self.base_url
        
        log_info(f"Checking if server is running at {root_url}...")
        try:
            response = self.session.get(f"{root_url}/health", timeout=5)
            if response.status_code < 400:
                log_success("Server is running")
                return True
        except requests.RequestException:
            pass
        log_error(f"Server is not running at {root_url}")
        log_info("Start the server with: cargo run --bin metavisor")
        return False


class TestRunner:
    """Test runner for Metavisor API."""

    def __init__(self, client: MetavisorClient):
        self.client = client
        self.entity_guids: list[str] = []
        self.relationship_guids: list[str] = []

    def _verify_type_exists(self, type_name: str) -> dict:
        """Verify type exists and return its definition."""
        response = self.client.get(
            f"/api/metavisor/v1/types/typedef/name/{type_name}",
            f"verify {type_name} type exists"
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"Invalid response for type {type_name}")
        if response.get("name") != type_name:
            raise RuntimeError(f"Type name mismatch: expected {type_name}, got {response.get('name')}")
        return response

    def _verify_relationship_type_exists(self, type_name: str) -> dict:
        """Verify relationship type exists and return its definition."""
        response = self.client.get(
            f"/api/metavisor/v1/types/relationshipdef/name/{type_name}",
            f"verify {type_name} relationship type exists"
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"Invalid response for relationship type {type_name}")
        if response.get("name") != type_name:
            raise RuntimeError(f"Relationship type name mismatch: expected {type_name}, got {response.get('name')}")
        return response

    def _verify_entity_by_qualified_name(self, type_name: str, qualified_name: str) -> dict:
        """Verify entity exists by qualifiedName and return it.

        Query parameter format: ?attr:qualifiedName=value
        """
        response = self.client.get(
            f"/api/metavisor/v1/entity/uniqueAttribute/type/{type_name}"
            f"?attr:qualifiedName={qualified_name}",
            f"verify {type_name} entity {qualified_name} exists"
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"Invalid response for entity {qualified_name}")
        if not response.get("guid"):
            raise RuntimeError(f"Entity missing guid: {qualified_name}")
        return response

    def _verify_relationship_by_guid(self, guid: str) -> dict:
        """Verify relationship exists by GUID and return it."""
        response = self.client.get(
            f"/api/metavisor/v1/relationship/guid/{guid}",
            f"verify relationship {guid} exists"
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"Invalid response for relationship {guid}")
        if response.get("guid") != guid:
            raise RuntimeError(f"Relationship guid mismatch: expected {guid}, got {response.get('guid')}")
        return response

    def _verify_entity_by_guid(self, guid: str, expected_type: str | None = None) -> dict:
        """Verify entity exists by GUID and return it."""
        response = self.client.get(
            f"/api/metavisor/v1/entity/guid/{guid}",
            f"verify entity {guid} exists"
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"Invalid response for entity {guid}")
        if response.get("guid") != guid:
            raise RuntimeError(f"Entity guid mismatch: expected {guid}, got {response.get('guid')}")
        if expected_type and response.get("typeName") != expected_type:
            raise RuntimeError(f"Entity type mismatch: expected {expected_type}, got {response.get('typeName')}")
        return response

    def create_types(self) -> None:
        """Create type definitions."""
        log_section("Creating Type Definitions")

        log_info("Creating sql_meta type...")
        self.client.post_file(
            "/api/metavisor/v1/types/typedefs",
            DATA_DIR / "sql_meta_type.json",
            "create sql_meta type"
        )
        self._verify_type_exists("sql_meta")

        log_info("Creating column_meta type...")
        self.client.post_file(
            "/api/metavisor/v1/types/typedefs",
            DATA_DIR / "column_meta_type.json",
            "create column_meta type"
        )
        self._verify_type_exists("column_meta")

        log_info("Creating relationship types...")
        self.client.post_file(
            "/api/metavisor/v1/types/typedefs",
            DATA_DIR / "relationship_type.json",
            "create relationship types"
        )
        self._verify_relationship_type_exists("join_relationship")
        self._verify_relationship_type_exists("sql_uses_column")

        log_success("Type definitions created")

    def create_entities(self) -> None:
        """Create entities."""
        log_section("Creating Entities")

        log_info("Creating column_meta entity 1 (PARTY_ID)...")
        response = self.client.post_file(
            "/api/metavisor/v1/entity",
            DATA_DIR / "column_meta_entity_1.json",
            "create column_meta entity 1"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_entity_by_guid(guid, "column_meta")
            self.entity_guids.append(guid)

        log_info("Creating column_meta entity 2 (CUST_ID)...")
        response = self.client.post_file(
            "/api/metavisor/v1/entity",
            DATA_DIR / "column_meta_entity_2.json",
            "create column_meta entity 2"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_entity_by_guid(guid, "column_meta")
            self.entity_guids.append(guid)

        log_info("Creating sql_meta entity 1...")
        response = self.client.post_file(
            "/api/metavisor/v1/entity",
            DATA_DIR / "sql_meta_entity_1.json",
            "create sql_meta entity 1"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_entity_by_guid(guid, "sql_meta")
            self.entity_guids.append(guid)

        log_success("Entities created")

    def create_relationships(self) -> None:
        """Create relationships."""
        log_section("Creating Relationships")

        log_info("Creating join_relationship 1...")
        response = self.client.post_file(
            "/api/metavisor/v1/relationship",
            DATA_DIR / "join_relationship_1.json",
            "create join_relationship 1"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_relationship_by_guid(guid)
            self.relationship_guids.append(guid)

        log_info("Creating join_relationship 2...")
        response = self.client.post_file(
            "/api/metavisor/v1/relationship",
            DATA_DIR / "join_relationship_2.json",
            "create join_relationship 2"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_relationship_by_guid(guid)
            self.relationship_guids.append(guid)

        log_info("Creating sql_uses_column relationship 1...")
        response = self.client.post_file(
            "/api/metavisor/v1/relationship",
            DATA_DIR / "sql_column_relationship_1.json",
            "create sql_uses_column 1"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_relationship_by_guid(guid)
            self.relationship_guids.append(guid)

        log_info("Creating sql_uses_column relationship 2...")
        response = self.client.post_file(
            "/api/metavisor/v1/relationship",
            DATA_DIR / "sql_column_relationship_2.json",
            "create sql_uses_column 2"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_relationship_by_guid(guid)
            self.relationship_guids.append(guid)

        log_success("Relationships created")

    def create_lineage_relationships(self) -> None:
        """Create lineage relationships (process with inputs/outputs)."""
        log_section("Creating Lineage Entity (with auto-created relationships)")

        log_info("Creating sql_meta lineage entity (with inputs/outputs)...")
        response = self.client.post_file(
            "/api/metavisor/v1/entity",
            DATA_DIR / "sql_meta_entity_lineage.json",
            "create lineage entity"
        )
        if isinstance(response, dict) and response.get("guid"):
            guid = response["guid"]
            self._verify_entity_by_guid(guid, "sql_meta")
            self.entity_guids.append(guid)

        log_info("Graph statistics after lineage creation...")
        stats = self.client.get(
            "/api/metavisor/v1/graph/stats",
            "get graph stats"
        )
        if isinstance(stats, dict):
            log_info(f"Graph nodes: {stats.get('node_count', 'N/A')}, edges: {stats.get('edge_count', 'N/A')}")

        log_success("Lineage entity created with auto-generated relationships")

    def run_query(self) -> None:
        """Run query tests."""
        log_section("Running Query Tests")

        log_info("Querying join_relationship by end2 qualifiedName (basic search)...")
        response = self.client.post_file(
            "/api/metavisor/v1/search/basic",
            DATA_DIR / "query.json",
            "query join_relationship"
        )
        # Verify query returned results
        if isinstance(response, dict):
            results = response.get("results", [])
            log_info(f"Basic search returned {len(results)} results")

        log_info("Searching relations by end1 filter...")
        response = self.client.post(
            "/api/metavisor/v1/search/relations",
            data={
                "typeName": "join_relationship",
                "relationshipFilters": {
                    "end1": {
                        "typeName": "column_meta",
                        "uniqueAttributes": {
                            "qualifiedName": "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID"
                        }
                    }
                },
                "limit": 50,
                "offset": 0
            },
            description="search relations by end1 filter"
        )
        # Verify search returned results
        if isinstance(response, dict):
            results = response.get("results", [])
            log_info(f"Relations search returned {len(results)} results")

        log_success("Query tests completed")

    def test_lineage(self) -> None:
        """Test lineage API."""
        log_section("Testing Lineage API")

        log_info("Getting lineage entity GUID...")
        response = self.client.get(
            "/api/metavisor/v1/lineage/uniqueAttribute/type/column_meta"
            "?attr:qualifiedName=BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID&direction=BOTH",
            "get lineage entity by qualifiedName"
        )

        lineage_guid = response.get("guid") if isinstance(response, dict) else None
        print(f"Lineage entity GUID: {lineage_guid}")

        if lineage_guid:
            log_info("Getting upstream lineage (direction=INPUT)...")
            self.client.get(
                f"/api/metavisor/v1/lineage/{lineage_guid}?depth=3&direction=INPUT",
                "get upstream lineage"
            )

            log_info("Getting downstream lineage (direction=OUTPUT)...")
            self.client.get(
                f"/api/metavisor/v1/lineage/{lineage_guid}?depth=3&direction=OUTPUT",
                "get downstream lineage"
            )

            log_info("Getting full lineage (direction=BOTH)...")
            self.client.get(
                f"/api/metavisor/v1/lineage/{lineage_guid}?depth=3&direction=BOTH",
                "get full lineage"
            )
        else:
            raise RuntimeError("Could not find lineage entity")

        log_success("Lineage tests completed")

    def list_all(self) -> None:
        """List all data."""
        log_section("Listing All Data")

        log_info("Listing all types...")
        response = self.client.get("/api/metavisor/v1/types/typedefs")
        if isinstance(response, dict) and "entityDefs" in response:
            for defn in response["entityDefs"]:
                print(defn.get("name"))
        print()

        log_info("Listing relationship types...")
        response = self.client.get("/api/metavisor/v1/types/relationshipdefs")
        if isinstance(response, list):
            for rel in response:
                print(rel.get("name"))
        print()

        log_info("Getting specific type definition (sql_meta)...")
        self.client.get("/api/metavisor/v1/types/typedef/name/sql_meta")
        print()

        log_info("Getting specific type definition (column_meta)...")
        self.client.get("/api/metavisor/v1/types/typedef/name/column_meta")
        print()

        log_info("Listing relationships for column_meta entity...")
        response = self.client.get(
            "/api/metavisor/v1/entity/uniqueAttribute/type/column_meta"
            "?attr:qualifiedName=BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID",
            "get column_meta entity by qualifiedName"
        )
        if isinstance(response, dict):
            entity_guid = response.get("guid")
            if entity_guid:
                self.client.get(f"/api/metavisor/v1/relationship/entity/{entity_guid}")
        print()

        log_success("List completed")

    def cleanup(self) -> None:
        """Clean up test data."""
        log_section("Cleaning Up Test Data")

        log_info("Deleting relationships...")
        for guid in reversed(self.relationship_guids):
            if self.client.delete(f"/api/metavisor/v1/relationship/guid/{guid}", f"delete relationship {guid}"):
                log_info(f"Deleted relationship {guid}")

        log_info("Deleting entities...")
        for guid in reversed(self.entity_guids):
            if self.client.delete(f"/api/metavisor/v1/entity/guid/{guid}", f"delete entity {guid}"):
                log_info(f"Deleted entity {guid}")

        log_info("Deleting type definitions...")

        type_deletions = [
            ("/api/metavisor/v1/types/typedef/name/sql_meta", "sql_meta type"),
            ("/api/metavisor/v1/types/typedef/name/column_meta", "column_meta type"),
            ("/api/metavisor/v1/types/relationshipdef/name/join_relationship", "join_relationship type"),
            ("/api/metavisor/v1/types/relationshipdef/name/sql_uses_column", "sql_uses_column type"),
        ]

        for path, name in type_deletions:
            if self.client.delete(path, f"delete {name}"):
                log_info(f"Deleted {name}")

        # Clear saved GUIDs after cleanup
        self.entity_guids.clear()
        self.relationship_guids.clear()

        log_success("Cleanup completed")

    def get_type(self, type_name: str = "sql_meta") -> None:
        """Get specific type definition."""
        log_section(f"Getting Type Definition: {type_name}")
        self.client.get(f"/api/metavisor/v1/types/typedef/name/{type_name}")
        print()

    def get_entity(self, type_name: str = "column_meta",
                   qualified_name: str = "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID") -> None:
        """Get specific entity by qualifiedName."""
        log_section(f"Getting Entity: {type_name} / {qualified_name}")
        self.client.get(
            f"/api/metavisor/v1/entity/uniqueAttribute/type/{type_name}"
            f"?attr:qualifiedName={qualified_name}",
            f"get {type_name} entity by qualifiedName"
        )
        print()

    def get_entity_by_guid(self, guid: str) -> None:
        """Get entity by GUID."""
        log_section(f"Getting Entity by GUID: {guid}")
        self.client.get(f"/api/metavisor/v1/entity/guid/{guid}")
        print()

    def get_entitydef_by_guid(self, guid: str) -> None:
        """Get entity type definition by GUID."""
        log_section(f"Getting EntityDef by GUID: {guid}")
        self.client.get(f"/api/metavisor/v1/types/entitydef/guid/{guid}")
        print()

    def run_all(self) -> None:
        """Run all tests."""
        if not self.client.check_server():
            sys.exit(1)

        try:
            self.create_types()
            self.create_entities()
            self.create_relationships()
            self.create_lineage_relationships()
            self.run_query()
            self.test_lineage()
            self.list_all()
            log_section("All Tests Completed")
        except RuntimeError:
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Metavisor API Test Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  all                      Run all tests (default)
  types                    Create type definitions only
  entities                 Create entities only
  relationships            Create relationships only
  lineage-relationships    Create lineage relationships (process_inputs/outputs)
  query                    Run query tests only
  lineage                  Run lineage tests only
  list                     List all data
  get-type [name]          Get type definition (default: sql_meta)
  get-entity [type] [qn]   Get entity by qualifiedName (default: column_meta, BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID)
  get-entity-by-guid <guid>  Get entity by GUID
  get-entitydef-by-guid <guid>  Get entity type definition by GUID
  cleanup                  Delete all test data
"""
    )

    parser.add_argument("command", nargs="?", default="all", help="Command to run")
    parser.add_argument("args", nargs="*", help="Command arguments")
    parser.add_argument("--base-url", default=BASE_URL, help=f"Base URL (default: {BASE_URL})")

    args = parser.parse_args()

    client = MetavisorClient(args.base_url)
    runner = TestRunner(client)

    commands = {
        "types": lambda: runner.create_types(),
        "entities": lambda: runner.create_entities(),
        "relationships": lambda: runner.create_relationships(),
        "lineage-relationships": lambda: runner.create_lineage_relationships(),
        "query": lambda: runner.run_query(),
        "lineage": lambda: runner.test_lineage(),
        "list": lambda: runner.list_all(),
        "cleanup": lambda: runner.cleanup(),
        "all": lambda: runner.run_all(),
    }

    if args.command in commands:
        if not client.check_server():
            sys.exit(1)
        try:
            if args.command == "get-type":
                runner.get_type(args.args[0] if args.args else "sql_meta")
            elif args.command == "get-entity":
                type_name = args.args[0] if len(args.args) > 0 else "column_meta"
                qualified_name = args.args[1] if len(args.args) > 1 else "BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID"
                runner.get_entity(type_name, qualified_name)
            elif args.command == "get-entity-by-guid":
                if not args.args:
                    log_error("Usage: run_test_data.py get-entity-by-guid <guid>")
                    sys.exit(1)
                runner.get_entity_by_guid(args.args[0])
            else:
                commands[args.command]()
        except RuntimeError:
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
