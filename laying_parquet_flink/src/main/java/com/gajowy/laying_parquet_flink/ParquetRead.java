package com.gajowy.laying_parquet_flink;

import com.google.common.base.Preconditions;
import com.gajowy.laying_parquet_flink.avro.Record;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;

public class ParquetRead {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String filenamePrefix = parameter.get("filenamePrefix");
    Preconditions.checkNotNull(filenamePrefix);

    readParquet(getExecutionEnvironment(),filenamePrefix).print();
  }

  private static DataSource readParquet(
    ExecutionEnvironment env, String inputPath) throws IOException {

    Job job = Job.getInstance();

    HadoopInputFormat<Void, Record> hadoopInputFormat
      = new HadoopInputFormat<>(new AvroParquetInputFormat<>(), Void.class, Record.class, job);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    AvroParquetInputFormat.setAvroReadSchema(job, Record.getClassSchema());

    return env.createInput(hadoopInputFormat);
  }
}
