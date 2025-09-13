import asyncio
import pytest
from unittest.mock import MagicMock, ANY, AsyncMock

from maga.crawler import Maga
from maga import utils
from maga import constants
import config

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

@pytest.fixture
def loop():
    """Create and provide a new asyncio event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def crawler(loop):
    """Create a new instance of the Maga crawler for each test."""
    return Maga(loop=loop, bootstrap_nodes=[])

async def test_handle_find_node_query(crawler, monkeypatch):
    """
    Test that when a find_node query is received, the crawler responds
    with the K closest nodes from its routing table.
    """
    # 1. Populate k-buckets with more than K nodes to ensure we get exactly K back
    # Mock _send_query_and_wait to prevent real network calls during _add_node
    monkeypatch.setattr(crawler, "_send_query_and_wait", AsyncMock(return_value=None))
    for i in range(config.K + 5):
        # Make IDs somewhat predictable for debugging
        node_id = b'\xaa' * 19 + bytes([i])
        await crawler._add_node(node_id, (f"1.1.1.{i}", 1111))

    # 2. Mock the send_message method
    send_message_mock = MagicMock()
    monkeypatch.setattr(crawler, "send_message", send_message_mock)

    # 3. Simulate a find_node query
    target_id = b'\xaa' * 20  # A target close to our nodes
    querying_node_id = b'\xbb' * 20
    querying_addr = ("2.2.2.2", 2222)
    tid = b't1'

    query_args = {
        constants.KRPC_ID: querying_node_id,
        constants.KRPC_TARGET: target_id
    }

    await crawler.handle_query(tid, constants.KRPC_FIND_NODE, query_args, querying_addr)

    # 4. Assert the response
    send_message_mock.assert_called_once()

    # Get the arguments passed to send_message
    response_call = send_message_mock.call_args
    response_data = response_call.args[0]
    response_addr = response_call.kwargs['addr']

    assert response_addr == querying_addr
    assert response_data[constants.KRPC_T] == tid
    assert response_data[constants.KRPC_Y] == constants.KRPC_RESPONSE

    response_r = response_data[constants.KRPC_R]
    assert constants.KRPC_NODES in response_r

    # 5. Verify the content of the 'nodes' response
    unpacked_nodes = list(utils.split_nodes(response_r[constants.KRPC_NODES]))

    # We should get K nodes back
    assert len(unpacked_nodes) == config.K

    # The returned nodes should be the closest ones to the target_id
    all_nodes = []
    for bucket in crawler.k_buckets:
        all_nodes.extend(bucket)

    # Sort all nodes by distance to find the true K closest
    all_nodes.sort(key=lambda n: utils.get_distance(n['id'], target_id))
    expected_closest_nodes = all_nodes[:config.K]
    expected_node_ids = {n['id'] for n in expected_closest_nodes}

    returned_node_ids = {n[0] for n in unpacked_nodes}

    assert returned_node_ids == expected_node_ids
