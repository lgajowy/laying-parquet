package com.gajowy.laying_parquet_spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParquetWrite {

  private static StructType schema = new StructType(
    new StructField[] { new StructField("row", DataTypes.StringType, true, Metadata.empty()) });

  public static void main(String[] args) {
    if (args.length < 2) {
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("Write parquet files").getOrCreate();

    SQLContext sqlContext = new SQLContext(spark);

    List<Row> records = IntStream.range(0, Integer.valueOf(args[1])).boxed()
      .map(i -> String.format("Spark created this file. Line no. %s", i)).map(RowFactory::create)
      .collect(Collectors.toList());

    Dataset dataset = sqlContext.createDataFrame(records, schema);
    dataset.write().parquet(args[0]);
    dataset.show(1000, false);
    spark.close();
  }
}
