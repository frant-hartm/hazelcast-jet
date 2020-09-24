/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSourceFactory;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import com.hazelcast.jet.pipeline.file.RawBytesFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.hadoop.impl.CsvInputFormat.CSV_INPUT_FORMAT_BEAN_CLASS;
import static com.hazelcast.jet.hadoop.impl.JsonInputFormat.JSON_INPUT_FORMAT_BEAN_CLASS;

/**
 * Hadoop based implementation for FileSourceFactory
 *
 * @param <T> type of the items emitted from the source
 */
public class HadoopSourceFactory<T> implements FileSourceFactory<T> {

    private final Map<Class<? extends FileFormat>, JobConfigurer<?, ?>> configs;

    /**
     * Creates HadoopSourceFactory
     */
    public HadoopSourceFactory() {
        configs = new HashMap<>();

        configs.put(AvroFileFormat.class, new AvroFormatJobConfigurer());
        configs.put(CsvFileFormat.class, new CsvFormatJobConfigurer());
        configs.put(JsonFileFormat.class, new JsonFormatJobConfigurer());
        configs.put(LinesTextFileFormat.class, new LineTextJobConfigurer());
        configs.put(ParquetFileFormat.class, new ParquetFormatJobConfigurer());
        configs.put(RawBytesFileFormat.class, new RawBytesFormatJobConfigurer());
        configs.put(TextFileFormat.class, new TextJobConfigurer());
    }

    @Override
    public BatchSource<T> create(FileSourceBuilder<T> builder) {

        try {
            Job job = Job.getInstance();
            FileInputFormat.addInputPath(job, new Path(builder.path()));

            FileFormat<T> fileFormat = builder.format();
            JobConfigurer<FileFormat, BiFunctionEx<?, ?, T>> configurer =
                    (JobConfigurer<FileFormat, BiFunctionEx<?, ?, T>>) configs.get(fileFormat.getClass());
            configurer.configure(job, fileFormat);

            Configuration configuration = job.getConfiguration();
            return HadoopSources.inputFormat(configuration, configurer.projectionFn());
        } catch (IOException e) {
            throw new JetException("Could not create a source", e);
        }
    }

    public interface JobConfigurer<F, Fn> {

        void configure(Job job, F format);

        Fn projectionFn();
    }

    private static class AvroFormatJobConfigurer implements
            JobConfigurer<AvroFileFormat<?>, BiFunctionEx<AvroKey<?>, NullWritable, ?>> {

        @Override
        public void configure(Job job, AvroFileFormat<?> format) {
            job.setInputFormatClass(AvroKeyInputFormat.class);

            Class<?> reflectClass = format.reflectClass();
            if (reflectClass != null) {
                Schema schema = ReflectData.get().getSchema(reflectClass);
                AvroJob.setInputKeySchema(job, schema);
            }
        }

        @Override
        public BiFunctionEx<AvroKey<?>, NullWritable, ?> projectionFn() {
            return (k, v) -> k.datum();
        }
    }

    private static class RawBytesFormatJobConfigurer implements
            JobConfigurer<RawBytesFileFormat, BiFunctionEx<NullWritable, BytesWritable, byte[]>> {


        @Override
        public void configure(Job job, RawBytesFileFormat format) {
            job.setInputFormatClass(WholeFileInputFormat.class);
        }

        @Override
        public BiFunctionEx<NullWritable, BytesWritable, byte[]> projectionFn() {
            return (k, v) -> v.copyBytes();
        }

    }

    private static class CsvFormatJobConfigurer
            implements JobConfigurer<CsvFileFormat<?>, BiFunctionEx<NullWritable, Object, Object>> {

        @Override
        public void configure(Job job, CsvFileFormat<?> format) {
            job.setInputFormatClass(CsvInputFormat.class);
            job.getConfiguration().set(CSV_INPUT_FORMAT_BEAN_CLASS, format.clazz().getCanonicalName());
        }

        @Override
        public BiFunctionEx<NullWritable, Object, Object> projectionFn() {
            return (k, v) -> v;
        }
    }

    private static class JsonFormatJobConfigurer
            implements JobConfigurer<JsonFileFormat<?>, BiFunctionEx<LongWritable, ?, ?>> {

        @Override
        public void configure(Job job, JsonFileFormat<?> format) {
            job.setInputFormatClass(JsonInputFormat.class);
            job.getConfiguration().set(JSON_INPUT_FORMAT_BEAN_CLASS, format.clazz().getCanonicalName());
        }

        @Override
        public BiFunctionEx<LongWritable, ?, ?> projectionFn() {
            return (k, v) -> v;
        }
    }


    private static class LineTextJobConfigurer
            implements JobConfigurer<LinesTextFileFormat, BiFunctionEx<LongWritable, Text, String>> {

        @Override
        public void configure(Job job, LinesTextFileFormat format) {
            job.setInputFormatClass(TextInputFormat.class);
        }

        @Override
        public BiFunctionEx<LongWritable, Text, String> projectionFn() {
            return (k, v) -> v.toString();
        }
    }

    private static class ParquetFormatJobConfigurer
            implements JobConfigurer<ParquetFileFormat<?>, BiFunctionEx<String, ?, ?>> {

        @Override
        public void configure(Job job, ParquetFileFormat<?> format) {
            job.setInputFormatClass(AvroParquetInputFormat.class);
        }

        @Override
        public BiFunctionEx<String, ?, ?> projectionFn() {
            return (k, v) -> v;
        }
    }

    private static class TextJobConfigurer
            implements JobConfigurer<TextFileFormat, BiFunctionEx<NullWritable, Text, String>> {

        @Override
        public void configure(Job job, TextFileFormat format) {
            job.setInputFormatClass(WholeTextInputFormat.class);
        }

        @Override
        public BiFunctionEx<NullWritable, Text, String> projectionFn() {
            return (k, v) -> v.toString();
        }
    }

}