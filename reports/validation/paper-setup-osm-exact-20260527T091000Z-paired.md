# Paired Paper Setup Comparison: `paper-setup-osm-exact-20260527T091000Z`

Generated: 2026-05-27T09:07:58.747648+00:00

| Dataset | Setup | Runtime | Status |
|---|---|---:|---|
| Bangladesh road / OSM (smoke input) | Published ICCIT Hadoop baseline | 56,274 ms | reference only |
| Bangladesh road / OSM (smoke input) | Published ICCIT Hadoop AES+DSCP | 42,366 ms | reference only (24.7% published reduction) |
| Bangladesh road / OSM (smoke input) | Spark Rai-Lian indexed baseline (`baseline`) | 786 ms | observed current-machine control |
| Bangladesh road / OSM (smoke input) | Spark ICCIT AES+DSCP (`aes-dscp`) | 725 ms | observed current-machine upgrade |

Within-Spark reduction against the Rai-Lian indexed baseline: **7.76%**.

Published Hadoop milliseconds are not combined with Spark timings as an engine speedup because
the hardware and runtime conditions differ.
