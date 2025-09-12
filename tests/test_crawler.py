import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, ANY

from maga.crawler import Maga, K
from maga import utils

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
    # We don't need bootstrap nodes for these unit tests
    return Maga(loop=loop, bootstrap_nodes=[])


async def test_rate_limiter_drops_packets(crawler, monkeypatch):
    """Test that the rate limiter drops packets from an IP that exceeds the limit."""
    # Patch constants for the test
    monkeypatch.setattr("maga.crawler.constants.RATE_LIMIT_REQUESTS", 3)
    monkeypatch.setattr("maga.crawler.constants.RATE_LIMIT_WINDOW", 1)

    from fastbencode import bencode

    crawler.handle_message = MagicMock()

    attacker_addr = ("1.1.1.1", 1111)

    valid_message = {
        b't': b'tt',
        b'y': b'q',
        b'q': b'ping',
        b'a': {b'id': utils.random_node_id()}
    }
    some_data = bencode(valid_message)

    # Send 3 packets, should be accepted
    for _ in range(3):
        crawler.datagram_received(some_data, attacker_addr)

    assert crawler.handle_message.call_count == 3

    # Send a 4th packet, should be dropped
    crawler.datagram_received(some_data, attacker_addr)
    assert crawler.handle_message.call_count == 3 # Still 3

    # Wait for the window to expire
    await asyncio.sleep(1.1)

    # Send another packet, should be accepted now
    crawler.datagram_received(some_data, attacker_addr)
    assert crawler.handle_message.call_count == 4
