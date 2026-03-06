#!/bin/bash
# Test script for Type CRUD API
# Usage: ./test_type_crud.sh [base_url]

set -e

BASE_URL="${1:-http://localhost:31000}"
DATA_DIR="$(dirname "$0")/data"
TYPE_FILE="$DATA_DIR/column_meta_type.json"

echo "=== Testing Type CRUD API ==="
echo "Base URL: $BASE_URL"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

pass() {
    echo -e "${GREEN}✓ PASS${NC}: $1"
}

fail() {
    echo -e "${RED}✗ FAIL${NC}: $1"
    exit 1
}

# 1. Health check
echo "1. Testing health check..."
RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/health")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ] && [ "$BODY" = "OK" ]; then
    pass "Health check returned 200 OK"
else
    fail "Health check failed: HTTP $HTTP_CODE, Body: $BODY"
fi

# 2. Get all types (should be empty initially or return existing)
echo ""
echo "2. Testing get all types..."
RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/api/metavisor/v1/types/typedefs")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    pass "Get all types returned 200"
    echo "   Response: $BODY"
else
    fail "Get all types failed: HTTP $HTTP_CODE"
fi

# 3. Create type from JSON file
echo ""
echo "3. Testing create type from column_meta_type.json..."
RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -d @"$TYPE_FILE" \
    "$BASE_URL/api/metavisor/v1/types/typedefs")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "201" ]; then
    pass "Create type returned 201"
    echo "   Response: $BODY"
else
    fail "Create type failed: HTTP $HTTP_CODE, Body: $BODY"
fi

# 4. Get type by name
echo ""
echo "4. Testing get type by name (column_meta)..."
RESPONSE=$(curl -s -w "\n%{http_code}" \
    "$BASE_URL/api/metavisor/v1/types/typedef/name/column_meta")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    pass "Get type by name returned 200"
    # Verify the response contains the type name
    if echo "$BODY" | grep -q '"column_meta"'; then
        pass "Response contains 'column_meta' type"
    else
        fail "Response does not contain 'column_meta' type"
    fi
    echo "   Response: $BODY"
else
    fail "Get type by name failed: HTTP $HTTP_CODE, Body: $BODY"
fi

# 5. Get type headers
echo ""
echo "5. Testing get type headers..."
RESPONSE=$(curl -s -w "\n%{http_code}" \
    "$BASE_URL/api/metavisor/v1/types/typedefs/headers")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    pass "Get type headers returned 200"
    echo "   Response: $BODY"
else
    fail "Get type headers failed: HTTP $HTTP_CODE, Body: $BODY"
fi

# 6. Update type (add description)
echo ""
echo "6. Testing update type..."
UPDATE_JSON='{
  "entityDefs": [{
    "name": "column_meta",
    "superTypes": ["DataSet"],
    "serviceType": "atlas",
    "typeVersion": "1.1",
    "description": "Column metadata type - updated",
    "attributeDefs": [
      {"name": "column_id", "typeName": "string", "isOptional": false},
      {"name": "column_name", "typeName": "string", "isOptional": false},
      {"name": "table_name", "typeName": "string", "isOptional": false},
      {"name": "db_name", "typeName": "string", "isOptional": false},
      {"name": "new_attribute", "typeName": "string", "isOptional": true}
    ]
  }]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X PUT \
    -H "Content-Type: application/json" \
    -d "$UPDATE_JSON" \
    "$BASE_URL/api/metavisor/v1/types/typedefs")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    pass "Update type returned 200"
    echo "   Response: $BODY"
else
    fail "Update type failed: HTTP $HTTP_CODE, Body: $BODY"
fi

# 7. Verify update
echo ""
echo "7. Verifying update..."
RESPONSE=$(curl -s -w "\n%{http_code}" \
    "$BASE_URL/api/metavisor/v1/types/typedef/name/column_meta")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    if echo "$BODY" | grep -q 'new_attribute'; then
        pass "Update verified - new_attribute found"
    else
        fail "Update verification failed - new_attribute not found"
    fi
    echo "   Response: $BODY"
else
    fail "Verify update failed: HTTP $HTTP_CODE"
fi

# 8. Delete type
echo ""
echo "8. Testing delete type..."
DELETE_JSON='{
  "entityDefs": [{"name": "column_meta"}]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X DELETE \
    -H "Content-Type: application/json" \
    -d "$DELETE_JSON" \
    "$BASE_URL/api/metavisor/v1/types/typedefs")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "204" ]; then
    pass "Delete type returned 204"
else
    fail "Delete type failed: HTTP $HTTP_CODE"
fi

# 9. Verify deletion
echo ""
echo "9. Verifying deletion..."
RESPONSE=$(curl -s -w "\n%{http_code}" \
    "$BASE_URL/api/metavisor/v1/types/typedef/name/column_meta")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "404" ]; then
    pass "Deletion verified - type not found (404)"
else
    fail "Deletion verification failed: Expected 404, got HTTP $HTTP_CODE"
fi

echo ""
echo "=== All tests passed! ==="
