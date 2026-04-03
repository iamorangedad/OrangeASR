import pytest
import pytest_asyncio
import asyncio
import nats
import os
from nats.errors import TimeoutError

# --- Configuration ---
# If using kubectl port-forward, this is usually localhost:4222
NATS_URL = os.getenv("NATS_URL", "nats://10.0.0.27:30742")

# --- Fixtures: Resource Management ---


@pytest_asyncio.fixture
async def nats_client():
    """
    Initialize NATS connection.
    Each test case gets an independent connection, which is automatically closed after the test.
    """
    # 1. Establish connection
    try:
        nc = await nats.connect(NATS_URL)
    except Exception as e:
        pytest.fail(f"Cannot connect to NATS server: {e}")

    yield nc

    # 2. Cleanup resources (Teardown)
    # Flush buffer and close connection
    await nc.drain()


# --- Test Cases ---


@pytest.mark.asyncio
async def test_basic_pub_sub(nats_client):
    """
    Test 1: Basic Publish/Subscribe pattern (Fire and Forget)
    Verifies: Published messages are received by subscribers exactly as sent
    """
    topic = "tests.basic"
    payload = b"Hello NATS"

    # Create a Future object to capture async callback results
    # This is the standard approach for testing async callback functions
    future = asyncio.Future()

    async def message_handler(msg):
        # When a message is received, put the result into the future to unblock the test
        if not future.done():
            future.set_result(msg.data)

    # 1. Subscribe
    await nats_client.subscribe(topic, cb=message_handler)

    # 2. Publish
    await nats_client.publish(topic, payload)

    # 3. Assert
    try:
        # Wait for the future to have a result, up to 1 second
        received_data = await asyncio.wait_for(future, timeout=1.0)
        assert received_data == payload
    except asyncio.TimeoutError:
        pytest.fail("Test failed: subscriber did not receive the message within 1 second")


@pytest.mark.asyncio
async def test_request_reply(nats_client):
    """
    Test 2: Request/Reply pattern (Request-Reply)
    Verifies: Synchronous wait-for-ack functionality similar to HTTP
    """
    topic = "tests.service"
    request_data = b"Can you help?"
    response_data = b"Yes, I can"

    # Simulate a server: reply with specific data upon receiving a request
    async def service_handler(msg):
        # msg.respond replies directly to the sender
        await msg.respond(response_data)

    # 1. Start service subscription
    await nats_client.subscribe(topic, cb=service_handler)

    # 2. Send request
    # The request method blocks until a reply is received or it times out
    try:
        response = await nats_client.request(topic, request_data, timeout=1.0)

        # 3. Assert
        assert response.data == response_data
        print(f"\n[Pass] Got response: {response.data.decode()}")

    except TimeoutError:
        pytest.fail("Test failed: request timed out without receiving a reply")


@pytest.mark.asyncio
async def test_queue_group(nats_client):
    """
    Test 3: Queue Group - Load Balancing Test
    Verifies: Multiple messages are distributed among subscribers in the group, rather than each receiving all messages
    """
    topic = "tests.queue"
    queue_group_name = "workers"

    # Counter
    received_count = 0

    async def worker_handler(msg):
        nonlocal received_count
        received_count += 1

    # Start two subscribers in the same Queue Group
    await nats_client.subscribe(topic, queue=queue_group_name, cb=worker_handler)
    await nats_client.subscribe(topic, queue=queue_group_name, cb=worker_handler)

    # Send 10 messages
    for i in range(10):
        await nats_client.publish(topic, f"msg-{i}".encode())

    # Give some time for processing
    await asyncio.sleep(0.5)

    # In normal subscribe mode, 2 subscribers x 10 messages = 20 total receives
    # But in Queue Group mode, 10 messages are only processed 10 times (load balanced)
    assert received_count == 10
