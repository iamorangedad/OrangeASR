import pytest
import os
import uuid
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# --- Fixtures (Setup/Teardown) ---


@pytest.fixture(scope="module")
def mongo_uri():
    """
    Get the connection URI.
    Reads from MONGO_URI environment variable first, falls back to localhost (for port-forward testing).
    """
    return os.getenv("MONGO_URI", "mongodb://10.0.0.27:30327")


@pytest.fixture(scope="module")
def mongo_client(mongo_uri):
    """
    Establish database connection, automatically disconnect after tests complete.
    scope="module" means the connection is established only once for the entire test file for efficiency.
    """
    # Set 3-second connection timeout to avoid hanging on connection failure
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)

    yield client  # Tests run here

    # --- Teardown (cleanup after tests) ---
    try:
        # Clean up the temporary test database
        client.drop_database("pytest_check_db")
    except Exception:
        pass
    client.close()


@pytest.fixture(scope="function")
def test_collection(mongo_client):
    """
    Provide a clean collection for each test function.
    """
    db = mongo_client["pytest_check_db"]
    collection = db["smoke_test"]

    # Clear the collection before each test
    collection.delete_many({})

    yield collection

    # Optionally clear after the test as well
    collection.delete_many({})


# --- Test Cases ---


def test_connection_ping(mongo_client):
    """
    Test 1: Verify the server can be pinged (basic connectivity check)
    """
    try:
        # Admin command ping is the lowest-overhead check
        mongo_client.admin.command("ping")
    except ConnectionFailure as e:
        pytest.fail(f"Cannot connect to MongoDB, please check port-forward or network address. Error: {e}")


def test_write_permission(test_collection):
    """
    Test 2: Verify write permissions (key check for resolving Permission Denied issues)
    """
    sample_doc = {
        "_id": "write_check_001",
        "service": "asr-service",
        "status": "active",
    }

    try:
        result = test_collection.insert_one(sample_doc)
        assert result.inserted_id == "write_check_001"
    except OperationFailure as e:
        pytest.fail(f"Write failed, possibly due to insufficient /data directory permissions: {e}")


def test_read_consistency(test_collection):
    """
    Test 3: Verify written data can be read back (data integrity check)
    """
    # 1. Write
    unique_id = str(uuid.uuid4())
    payload = {"_id": unique_id, "content": "hello pytest"}
    test_collection.insert_one(payload)

    # 2. Read
    fetched_doc = test_collection.find_one({"_id": unique_id})

    # 3. Assert
    assert fetched_doc is not None, "Data just written was not found"
    assert fetched_doc["content"] == "hello pytest"


def test_server_info(mongo_client):
    """
    Test 4: Print server version info (optional, to confirm correct version downgrade)
    """
    server_info = mongo_client.server_info()
    version = server_info.get("version")
    print(f"\n[Info] MongoDB Version: {version}")

    # If you require version 4.4.x specifically
    assert version.startswith("4.4"), f"Version mismatch! Current version: {version}"
