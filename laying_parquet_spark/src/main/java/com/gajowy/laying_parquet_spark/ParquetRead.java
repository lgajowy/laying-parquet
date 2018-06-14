package com.gajowy.laying_parquet_spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetRead {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("Read parquet files")
      .getOrCreate();

    Dataset<Row> parquet = spark.read().parquet(args[0]);

    parquet.show(1000, false);

    spark.close();
  }

}
