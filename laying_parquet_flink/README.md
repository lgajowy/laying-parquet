# Read & write parquet files using flink 1.3.0:

## Build:

```
mvn clean install package
```

## Read:

```
./flink run --class com.gajowy.laying_parquet_flink.ParquetRead -m localhost:6123 /Users/lukasz/Projects/laying_parquet/laying_parquet_flink/target/laying_parquet_flink-1.0-SNAPSHOT-shaded.jar --filenamePrefix /Users/lukasz/parquet-demo/flink/parquet/tmp-r-00001.snappy.parquet
```


## Write:
```
/flink run --class com.gajowy.laying_parquet_flink.ParquetWrite -m localhost:6123 /Users/lukasz/Projects/laying_parquet/laying_parquet_flink/target/laying_parquet_flink-1.0-SNAPSHOT-shaded.jar --filenamePrefix /Users/lukasz/parquet-demo/flink/parquet/ --numberOfRecords 100
```


