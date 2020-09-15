package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.jet.json.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

import static com.hazelcast.jet.pipeline.file.JsonFileFormat.JSON_INPUT_FORMAT_BEAN_CLASS;

public class JsonInputFormat extends FileInputFormat<LongWritable, Object> {

    @Override
    public RecordReader<LongWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        try {
            Configuration configuration = context.getConfiguration();
            String className = configuration.get(JSON_INPUT_FORMAT_BEAN_CLASS);
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);

            return new RecordReader<LongWritable, Object>() {

                final LineRecordReader reader = new LineRecordReader();

                @Override
                public void initialize(
                        InputSplit split, TaskAttemptContext context
                ) throws IOException, InterruptedException {

                    reader.initialize(split, context);
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    return reader.nextKeyValue();
                }

                @Override
                public LongWritable getCurrentKey() throws IOException, InterruptedException {
                    return reader.getCurrentKey();
                }

                @Override
                public Object getCurrentValue() throws IOException, InterruptedException {
                    return JsonUtil.beanFrom(reader.getCurrentValue().toString(), clazz);
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return reader.getProgress();
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }
}
