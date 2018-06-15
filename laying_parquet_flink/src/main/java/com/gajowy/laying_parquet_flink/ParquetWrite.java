package com.gajowy.laying_parquet_flink;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.functions.MapFunction;
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

  private static final Schema SCHEMA = new Schema.Parser().parse("{\n"
    + " \"namespace\": \"testingParquet\",\n"
    + " \"type\": \"record\",\n"
    + " \"name\": \"TestAvroLine\",\n"
    + " \"fields\": [\n"
    + "     {\"name\": \"row\", \"type\": \"string\"}\n"
    + " ]\n"
    + "}");

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String filenamePrefix = parameter.get("filenamePrefix");
    String numberOfRecords = parameter.get("numberOfRecords");
    Preconditions.checkNotNull(filenamePrefix);
    Preconditions.checkNotNull(numberOfRecords);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    MapOperator<GenericData.Record, Tuple2<Void, GenericData.Record>> recordsInTuples
      = generateRecords(env, Integer.valueOf(numberOfRecords))
      .map(new MapToTuples())
      .returns(
        new TupleTypeInfo<>(TypeExtractor.getForClass(Void.class),
          TypeExtractor.getForClass(GenericData.Record.class)));

    writeParquet(recordsInTuples, filenamePrefix);
    env.execute();
  }

  private static void writeParquet(DataSet<Tuple2<Void, GenericData.Record>> data,
    String outputPath) throws IOException {
    Job job = Job.getInstance();

    HadoopOutputFormat<Void, GenericData.Record> hadoopOutputFormat = new HadoopOutputFormat<>(
      new AvroParquetOutputFormat<GenericData.Record>(), job);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    AvroParquetOutputFormat.setSchema(job, SCHEMA);
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetOutputFormat.setEnableDictionary(job, true);

    data.output(hadoopOutputFormat);
  }

  private static DataSource<GenericData.Record> generateRecords(ExecutionEnvironment env, Integer numberOfRecords) {
    List<GenericData.Record> records = IntStream.range(0, numberOfRecords).mapToObj(i -> {
      GenericData.Record record = new GenericData.Record(SCHEMA);
      record.put("row", String.format("Flink made this file. Number of row: %s", i));
      return record;

    }).collect(Collectors.toList());

    return env.fromCollection(records);
  }

  private static class MapToTuples implements MapFunction<GenericData.Record, Tuple2<Void, GenericData.Record>> {
    @Override public Tuple2<Void, GenericData.Record> map(GenericData.Record value) {
      return new Tuple2<>(null, value);
    }
  }
}
