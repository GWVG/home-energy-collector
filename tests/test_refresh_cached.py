"""Polling the slow weather sources: at most once per interval, keeping the
last good value when a fetch fails or returns nothing usable."""

import pytest

from collector import refresh_cached

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
    fetch = Fetcher({"temp_c": 20.0})
    cache, last = refresh_cached(None, None, INTERVAL, 5.0, fetch, "test")
    assert cache == {"temp_c": 20.0}
    assert last == 5.0


def test_no_fetch_within_interval():
    fetch = Fetcher()
    cache, last = refresh_cached({"temp_c": 20.0}, 0.0, INTERVAL, 1800.0, fetch, "test")
    assert fetch.calls == 0
    assert (cache, last) == ({"temp_c": 20.0}, 0.0)


def test_fetches_again_after_interval():
    fetch = Fetcher({"temp_c": 21.0})
    cache, last = refresh_cached({"temp_c": 20.0}, 0.0, INTERVAL, 3600.0, fetch, "test")
    assert cache == {"temp_c": 21.0}
    assert last == 3600.0


@pytest.mark.parametrize("outcome", [None, RuntimeError("site down")])
def test_failed_fetch_keeps_previous_value(outcome):
    fetch = Fetcher(outcome)
    cache, last = refresh_cached({"temp_c": 20.0}, 0.0, INTERVAL, 3600.0, fetch, "test")
    assert cache == {"temp_c": 20.0}
    assert last == 3600.0


@pytest.mark.parametrize("outcome", [None, RuntimeError("site down")])
def test_unusable_source_is_not_retried_every_cycle(outcome):
    # Regression: a station reporting "not online" (fetch returns None) used
    # to clear the cache, and an empty cache triggered a new fetch on the very
    # next 60-second cycle. Now the attempt is recorded and the next one
    # waits for the full interval.
    fetch = Fetcher(outcome)
    cache, last = None, None
    for now in range(0, 3600, 60):
        cache, last = refresh_cached(cache, last, INTERVAL, float(now), fetch, "test")
    assert fetch.calls == 1
    assert cache is None
