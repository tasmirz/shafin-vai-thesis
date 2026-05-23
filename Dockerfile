FROM apache/flink:2.2.0-scala_2.12

COPY target/probabilistic-topk-flink-1.0.0-SNAPSHOT-shaded.jar /opt/flink/usrlib/topk.jar
COPY datasets-raw /opt/flink/datasets-raw
