# Read & write parquet files using spark 2.2.0:

## Build:

```
mvn clean package
```

## Read:
```
spark-submit --class com.gajowy.laying_parquet_spark.ParquetRead --master spark://LGs-Mac.local:7077 /Users/lukasz/Projects/laying_parquet/laying_parquet_spark/target/laying_parquet_spark-1.0-SNAPSHOT.jar "/Users/lukasz/parquet-demo/spark/parquet/*"
```

## Write:

```
spark-submit --class com.gajowy.laying_parquet_spark.ParquetWrite --master spark://LGs-Mac.local:7077 /Users/lukasz/Projects/laying_parquet/laying_parquet_spark/target/laying_parquet_spark-1.0-SNAPSHOT.jar /Users/lukasz/parquet-demo/spark/parquet/ 1000
```

