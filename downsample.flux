import "types"

option task = {name: "downsample_telemetrie_15m", every: 15m, offset: 1m}

// Rolls raw data up into 15-minute means for long-term storage.
//
// Every 15 minutes, this processes the 15-minute slice that has just turned
// seven days old. Raw data is kept for 90 days, so if the task stalls there
// is ample time to notice and backfill. Small slices instead of one daily
// run keep the memory peak low: a full day of ~13,000 series filled the
// swap of the 2 GB container.
//
// Only numeric fields are aggregated: mean() fails on strings and booleans.
// Filtering on type instead of on field name means new numeric fields are
// picked up automatically. 15-minute windows match the grid operator's
// peak-demand windows.

from(bucket: "telemetrie")
  |> range(start: -7d15m, stop: -7d)
  |> filter(fn: (r) => types.isNumeric(v: r._value))
  |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
  |> to(bucket: "telemetrie_downsampled", org: "huis")
