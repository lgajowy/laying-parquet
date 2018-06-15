#

Demonstration of reading/writing Parquet files using flink, spark, beam. 

Since ParquetIO in beam is not released officially yet, use [this](https://github.com/lgajowy/beam/tree/spark-flink-parquet-example) repo's branch to run it.  



# Scenario

0. Set Aliases in shell (for convenience):

```
alias flink /Users/lukasz/Tools/flink-1.3.0/bin/flink
alias spark-submit /Users/lukasz/Tools/spark-2.2.0-bin-hadoop2.7/bin/spark-submit;
```


1. Build Beam (Direct runner)

```
cd /Users/lukasz/Projects/apache-beam/beam/examples/java
mvn clean package -Pdirect-runner -DskipTests
```

2. Write using Beam

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.parquet.ParquetWrite -Pdirect-runner -Dexec.args="--filenamePrefix=/Users/lukasz/parquet-demo/beam --numberOfRecords=1000"
```

(Delete extra unneeded files from the directories you just created, eg. .tmp dir)

3. Read using native Spark

```
spark-submit --class com.gajowy.laying_parquet_spark.ParquetRead --master spark://LGs-Mac.local:7077 /Users/lukasz/Projects/laying_parquet/laying_parquet_spark/target/laying_parquet_spark-1.0-SNAPSHOT.jar "/Users/lukasz/parquet-demo/beam/*"
```

4. Read using native Flink

```
flink run --class com.gajowy.laying_parquet_flink.ParquetRead -m localhost:6123 /Users/lukasz/Projects/laying_parquet/laying_parquet_flink/target/laying_parquet_flink-1.0-SNAPSHOT-shaded.jar --filenamePrefix "/Users/lukasz/parquet-demo/beam/*"
```


5. Write using native Spark:

```
spark-submit --class com.gajowy.laying_parquet_spark.ParquetWrite --master spark://LGs-Mac.local:7077 /Users/lukasz/Projects/laying_parquet/laying_parquet_spark/target/laying_parquet_spark-1.0-SNAPSHOT.jar /Users/lukasz/parquet-demo/spark/ 1000
```

6. Write using native Flink:

```
flink run --class com.gajowy.laying_parquet_flink.ParquetWrite -m localhost:6123 /Users/lukasz/Projects/laying_parquet/laying_parquet_flink/target/laying_parquet_flink-1.0-SNAPSHOT-shaded.jar --filenamePrefix /Users/lukasz/parquet-demo/flink/ --numberOfRecords 1000
```

7. Delete extra unneeded files from the directories you just created, eg. .tmp dir)


Flink:
```
rm -r _SUCCESS .*.crc _metadata _common_metadata
```

Spark:

```
rm -r _SUCCESS .*.crc
```

8. Read using beam (flink files):

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.parquet.ParquetRead -Pdirect-runner -Dexec.args="--filenamePrefix=/Users/lukasz/parquet-demo/flink/*"
```


9. Read using beam (spark files):

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.parquet.ParquetRead -Pdirect-runner -Dexec.args="--filenamePrefix=/Users/lukasz/parquet-demo/spark/*"
```

10. (optional) Read using spark/flink runner:

```
mvn clean package -Pspark-runner -DskipTests

spark-submit --class org.apache.beam.examples.parquet.ParquetRead --master spark://LGs-Mac.local:7077 /Users/lukasz/Projects/apache-beam/beam/examples/java/target/beam-examples-java-2.5.0-SNAPSHOT-shaded.jar --runner=SparkRunner --filenamePrefix="/Users/lukasz/parquet-demo/spark/*"

mvn clean package -Pflink-runner -DskipTests

// will run only on flink 1.4.0 (this one's supported in beam)

mvn exec:java -Dexec.mainClass=org.apache.beam.examples.parquet.ParquetRead -Pflink-runner -Dexec.args="--runner=FlinkRunner --flinkMaster=localhost:6123 --filesToStage=target/beam-examples-java-2.5.0-SNAPSHOT-shaded.jar --filenamePrefix=/Users/lukasz/parquet-demo/spark/*"



```

