FROM apache/spark:3.5.3-java17

USER root
WORKDIR /opt/spark/work-dir
RUN mkdir -p /opt/spark/app /opt/spark/datasets-raw
COPY datasets-raw /opt/spark/datasets-raw
COPY target/probabilistic-topk-spark-1.0.0-SNAPSHOT-shaded.jar /opt/spark/app/topk-spark.jar

# Keep the image usable for Spark master, worker, and spark-submit containers.
ENTRYPOINT []
CMD ["/opt/spark/bin/spark-submit", "--class", "com.thesis.topk.spark.ProbabilisticTopKSparkJob", "/opt/spark/app/topk-spark.jar"]
