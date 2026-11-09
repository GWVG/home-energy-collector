"""Polling the slow weather sources: at most once per interval, keeping the
last good reading when a fetch fails or returns nothing usable."""

import pytest

from collector import SlowSource

INTERVAL = 3600


class Fetcher:
    """Returns the queued results in order and counts the calls."""

    def __init__(self, *results):
        self.results = list(results)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_first_cycle_always_fetches():
    source = SlowSource(Fetcher({"temp_c": 20.0}), INTERVAL)
    assert source.get("test", 5.0) == {"temp_c": 20.0}


def test_no_fetch_within_interval():
    fetch = Fetcher({"temp_c": 20.0})
    source = SlowSource(fetch, INTERVAL)
    source.get("test", 0.0)
    assert source.get("test", 1800.0) == {"temp_c": 20.0}
    assert fetch.calls == 1


def test_fetches_again_after_interval():
    source = SlowSource(Fetcher({"temp_c": 20.0}, {"temp_c": 21.0}), INTERVAL)
    source.get("test", 0.0)
    assert source.get("test", 3600.0) == {"temp_c": 21.0}


@pytest.mark.parametrize("outcome", [None, RuntimeError("site down")])
def test_failed_fetch_keeps_previous_reading(outcome):
    source = SlowSource(Fetcher({"temp_c": 20.0}, outcome), INTERVAL)
    source.get("test", 0.0)
    assert source.get("test", 3600.0) == {"temp_c": 20.0}


@pytest.mark.parametrize("outcome", [None, RuntimeError("site down")])
def test_unusable_source_is_not_retried_every_cycle(outcome):
    # A station reporting "not online" (fetch returns None) must wait for the
    # full interval before the next attempt, not retry every 60-second cycle.
    fetch = Fetcher(outcome)
    source = SlowSource(fetch, INTERVAL)
    for now in range(0, 3600, 60):
        assert source.get("test", float(now)) is None
    assert fetch.calls == 1
