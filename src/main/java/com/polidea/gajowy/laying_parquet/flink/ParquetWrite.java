package com.polidea.gajowy.laying_parquet.flink;

import com.google.common.base.Preconditions;
import com.polidea.gajowy.laying_parquet.avro.Record;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
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
import java.util.Arrays;
import java.util.List;

public class ParquetWrite {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String filenamePrefix = parameter.get("filenamePrefix");
    Preconditions.checkNotNull(filenamePrefix);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    Record sampleRecord = generateSampleObject();

    DataSet<Tuple2<Void, Record>> output = putObjectIntoDataSet(env, sampleRecord);

    writeAvro(output, filenamePrefix);

    env.execute();
  }

  private static void writeAvro(DataSet<Tuple2<Void, Record>> data, String outputPath)
    throws IOException {
    Job job = Job.getInstance();

    // Set up Hadoop Output Format
    HadoopOutputFormat<Void, Record> hadoopOutputFormat
      = new HadoopOutputFormat<>(new AvroParquetOutputFormat<Record>(), job);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    AvroParquetOutputFormat.setSchema(job, Record.getClassSchema());
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetOutputFormat.setEnableDictionary(job, true);

    data.output(hadoopOutputFormat);
  }

  private static Record generateSampleObject() {
    Record record = new Record();
    record.setRow("Example Row");
    return record;
  }

  private static DataSet<Tuple2<Void, Record>> putObjectIntoDataSet(ExecutionEnvironment env,
    Record person) {
    List<Tuple2<Void, Record>> l = Arrays.asList(new Tuple2<Void, Record>(null, person));
    TypeInformation<Tuple2<Void, Record>> typeInfo
      = new TupleTypeInfo<>(TypeExtractor.getForClass(Void.class),
      TypeExtractor.getForClass(Record.class));

    DataSet<Tuple2<Void, Record>> data = env.fromCollection(l, typeInfo);

    return data;
  }

}
