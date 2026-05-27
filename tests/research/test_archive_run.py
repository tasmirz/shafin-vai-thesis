import unittest

from scripts.research.archive_run import parse_metrics


class ArchiveRunMetricsTest(unittest.TestCase):
  def test_interleaved_spark_warning_does_not_drop_query_metric(self):
    log = """engine=apache-spark source=simulator dataset=csv k=2 partitions=2 elapsedMs=9 algorithmElapsedMs=3 setupMs=6 validationMs=0 algorithm=aes-dscp dscp=true aes=true boundMode=rai-lian-artree-selected-level-partial-reducer emissionScope=server-partition
rawEvents=4 probabilisticInstances=4 synopsisRules=0 synopsisBins=4
TopKResult{engine=apache-spark, algorithm=aes-dscp, queryId=q0, objects=2, refined=2, pruned=0, pruneRatio=0.0000, tau=0.000000, emittedRecords=1, baselineEmissions=2, aesEmissions=1, AER=0.500000, falsePrunes=0, indexedMbrPath=true, partialMbrRefs=2, filterMs=1, emissionMs=1, refineMs=1, shuffleRecords=1, shuffleBytes=10, tasks=1, executorRunMs=1, gcMs=0, stragglerRatio=
26/05/26 16:22:14 WARN TaskSetManager: Stage contains a task of very large size
1.0000, validationPerformed=true, exactAgreement=true}
query=q0 algorithm=aes-dscp objects=2 refined=2 pruned=0 pruneRatio=0.0000 tau=0.000000 emittedRecords=1 baselineEmissions=2 aesEmissions=1 AER=0.500000 falsePrunes=0 indexedMbrPath=true partialMbrRefs=2 filterMs=1 emissionMs=1 refineMs=1 shuffleRecords=1 shuffleBytes=10 tasks=1 executorRunMs=1 gcMs=0 stragglerRatio=1.0000 validationPerformed=true exactAgreement=true
ObjectTrace{queryId=q0, objectId=obj-1, partition=1, lb=0.250000, ub=0.750000, tau=0.500000, decision=survived, partialMbrRefs=2, baselineEmissions=2, aesEmissions=1}
"""
    metrics = parse_metrics(log, "", None)

    self.assertEqual(1, len(metrics["spark"]["queries"]))
    self.assertTrue(metrics["spark"]["indexedMbrPath"])
    self.assertEqual(2, metrics["spark"]["totalPartialMbrRefs"])
    self.assertTrue(metrics["validation"]["exactTopKAgreement"])
    self.assertEqual("obj-1", metrics["spark"]["objectTraces"][0]["objectId"])
    self.assertEqual("survived", metrics["spark"]["objectTraces"][0]["decision"])

  def test_trace_without_dscp_threshold_serializes_as_json_null(self):
    log = """engine=apache-spark source=simulator dataset=csv k=2 partitions=2 elapsedMs=1 algorithmElapsedMs=1 setupMs=0 validationMs=0 algorithm=baseline dscp=false aes=false boundMode=rai-lian-artree-selected-level-partial-reducer emissionScope=server-partition
ObjectTrace{queryId=q0, objectId=obj-1, partition=1, lb=0.250000, ub=0.750000, tau=NaN, decision=survived, partialMbrRefs=2, baselineEmissions=2, aesEmissions=2}
"""
    metrics = parse_metrics(log, "", None)
    self.assertIsNone(metrics["spark"]["objectTraces"][0]["tau"])


if __name__ == "__main__":
  unittest.main()
