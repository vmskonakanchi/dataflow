import sys
import os
import pyarrow as pa
from fastapi.testclient import TestClient

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from api import app

def test_query_tool():
    client = TestClient(app)
    
    # 1. Test a valid query
    query_str = "SELECT 1 as id, 'Alice' as name, 23.5 as score"
    print(f"Testing valid query: {query_str}")
    response = client.post("/api/query", data={"query": query_str})
    
    if response.status_code != 200:
        print("Error response:", response.text)
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream", "Incorrect content-type header"

    
    # Read the response bytes as an Arrow stream
    reader = pa.ipc.open_stream(response.content)
    table = reader.read_all()
    
    print("Columns:", table.column_names)
    print("Schema:", table.schema)
    print("Data:")
    print(table.to_pydict())
    
    assert table.num_rows == 1
    assert table.column_names == ["id", "name", "score"]
    assert table["id"][0].as_py() == 1
    assert table["name"][0].as_py() == "Alice"
    assert table["score"][0].as_py() == 23.5
    print("Valid query test passed!")
    
    # 2. Test an invalid query
    invalid_query = "SELECT * FROM non_existent_table"
    print(f"\nTesting invalid query: {invalid_query}")
    response = client.post("/api/query", data={"query": invalid_query})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}"
    print("Invalid query returned 400: SUCCESS")
    print("Response detail:", response.json().get("detail"))

    # 3. Test default limit injection
    limit_test_query = "SELECT * FROM range(1000)"
    print(f"\nTesting default limit injection: {limit_test_query}")
    response = client.post("/api/query", data={"query": limit_test_query})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    reader = pa.ipc.open_stream(response.content)
    table = reader.read_all()
    print(f"Returned row count (expected 500): {table.num_rows}")
    assert table.num_rows == 500, f"Expected default limit to restrict rows to 500, got {table.num_rows}"
    print("Default limit injection test passed!")

    # 4. Test user-specified limit (overrides default limit)
    user_limit_query = "SELECT * FROM range(1000) LIMIT 750"
    print(f"\nTesting user-specified limit overrides default: {user_limit_query}")
    response = client.post("/api/query", data={"query": user_limit_query})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    reader = pa.ipc.open_stream(response.content)
    table = reader.read_all()
    print(f"Returned row count (expected 750): {table.num_rows}")
    assert table.num_rows == 750, f"Expected user limit to return 750 rows, got {table.num_rows}"
    print("User-specified limit test passed!")

if __name__ == "__main__":
    test_query_tool()
