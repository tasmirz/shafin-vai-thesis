# Paired Paper Setup Comparison: `paper-setup-role-exact-20260527T092000Z`

Generated: 2026-05-27T09:09:10.095364+00:00

| Dataset | Setup | Runtime | Status |
|---|---|---:|---|
| Bangladesh road / OSM (smoke input) | Published ICCIT Hadoop baseline | 56,274 ms | reference only |
| Bangladesh road / OSM (smoke input) | Published ICCIT Hadoop AES+DSCP | 42,366 ms | reference only (24.7% published reduction) |
| Bangladesh road / OSM (smoke input) | Spark Rai-Lian indexed baseline (`baseline`) | 926 ms | observed current-machine control |
| Bangladesh road / OSM (smoke input) | Spark ICCIT AES+DSCP (`aes-dscp`) | 865 ms | observed current-machine upgrade |

Within-Spark reduction against the Rai-Lian indexed baseline: **6.59%**.

Published Hadoop milliseconds are not combined with Spark timings as an engine speedup because
the hardware and runtime conditions differ.
