"""Cycle scheduling on the monotonic clock: a fixed start-to-start interval,
regardless of how long the fetches take."""

from collector import next_cycle_delay


def test_sleeps_only_the_rest_of_the_interval():
    # Cycle started at 100 and took 7 s: sleep 53 s, next start at 160.
    assert next_cycle_delay(100.0, 60, 107.0) == (160.0, 53.0)


def test_no_drift_over_many_cycles():
    scheduled = 0.0
    for _ in range(1000):
        scheduled, _delay = next_cycle_delay(scheduled, 60, scheduled + 7.0)
    assert scheduled == 60_000.0


def test_overrun_starts_next_cycle_now_without_catching_up():
    # Cycle started at 100 and took 75 s: no sleep, and the schedule restarts
    # from 175 instead of trying to fit in the missed slot.
    assert next_cycle_delay(100.0, 60, 175.0) == (175.0, 0.0)
