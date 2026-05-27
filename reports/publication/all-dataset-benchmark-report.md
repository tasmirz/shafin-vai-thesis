# Multi-Dataset Benchmark Evidence

Generated: 2026-05-27T14:08:31.030521+00:00

## Claim Boundary

CSV treatment rows compare the Spark indexed baseline with AES and DSCP treatments on
identical saved data and query-set checksums. MQTT/Kafka/Spark rows measure the bounded
stream ingress and execution route separately. Published Hadoop times are reference values
only; they are not combined with these Spark measurements as an engine speedup.

## CSV Treatment Matrix

| Dataset | Queries | Indexed baseline | AES-only | DSCP-only | AES+DSCP | Full reduction | Emission reduction | Shuffle reduction |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Synthetic smartphone | 20 | 41,800 ms | 37,050 ms | 43,303 ms | 36,527 ms | 12.61% | 74.68% | 32.27% |
| Bangladesh OSM (fixed 20 queries) | 20 | 917,176 ms | 519,036 ms | 603,290 ms | 379,637 ms | 58.61% | 98.86% | 49.76% |
| California/TIGER LA (exploratory 1 query) | 1 | 67,722 ms | 46,540 ms | 37,376 ms | 28,906 ms | 57.32% | 99.29% | 61.81% |

## MQTT -> Kafka -> Spark Matrix

| Dataset | Messages | Queries | Algorithm time | End-to-end time | Rate (msg/s) | Indexed MBR path | Exact validation |
|---|---:|---:|---:|---:|---:|---|---|
| Synthetic smartphone (full 20 queries) | 207,860 | 20 | 51,259 ms | 1,612,321 ms | 128.92 | true | not run |
| Bangladesh OSM (exact 40-object transport) | 310 | 1 | 2,529 ms | 16,181 ms | 19.16 | true | true |
| California/TIGER LA (exact 40-object transport) | 310 | 1 | 1,652 ms | 14,110 ms | 21.97 | true | true |

## Dataset Provenance

- Synthetic smartphone follows the ICCIT paper generator controls.
- Bangladesh OSM road is an ICCIT-style road-MBR artifact curated from the supplied OSM layer.
- California/TIGER LA roads use the supplied `tl_2018_06037_roads` layer and match the
  Rai-Lian object-count scale; they are not asserted to be the paper's exact California file.

## Files

- `reports/publication/all-dataset-treatment-matrix.csv`
- `reports/publication/all-dataset-stream-matrix.csv`
