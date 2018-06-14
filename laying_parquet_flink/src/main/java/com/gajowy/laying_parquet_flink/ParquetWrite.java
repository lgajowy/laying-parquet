package com.gajowy.laying_parquet_flink;

import com.google.common.base.Preconditions;
import com.gajowy.laying_parquet_flink.avro.Record;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParquetWrite {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String filenamePrefix = parameter.get("filenamePrefix");
    Preconditions.checkNotNull(filenamePrefix);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    MapOperator<Record, Tuple2<Void, Record>> recordsInTuples = generateRecords(env)
      .map(record -> new Tuple2<Void, Record>(null, record)).returns(
        new TupleTypeInfo<>(TypeExtractor.getForClass(Void.class),
          TypeExtractor.getForClass(Record.class)));
    writeParquet(recordsInTuples, filenamePrefix);

    env.execute();
  }

  private static void writeParquet(DataSet<Tuple2<Void, Record>> data, String outputPath)
    throws IOException {
    Job job = Job.getInstance();

    HadoopOutputFormat<Void, Record> hadoopOutputFormat = new HadoopOutputFormat<>(
      new AvroParquetOutputFormat<Record>(), job);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    AvroParquetOutputFormat.setSchema(job, Record.getClassSchema());
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetOutputFormat.setEnableDictionary(job, true);

    data.output(hadoopOutputFormat);
  }

  private static DataSource<Record> generateRecords(ExecutionEnvironment env) {
    List<Record> records = IntStream.range(0, 1000).mapToObj(i -> {
      Record record = new Record();
      record.setRow(String.format("Flink made this file. Number of row: %s", i));
      return record;

    }).collect(Collectors.toList());

    return env.fromCollection(records);
  }
}
