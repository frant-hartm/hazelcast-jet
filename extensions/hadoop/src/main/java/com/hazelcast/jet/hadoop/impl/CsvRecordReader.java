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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

import static com.hazelcast.jet.pipeline.file.CsvFileFormat.CSV_INPUT_FORMAT_BEAN_CLASS;

public class CsvRecordReader extends RecordReader<NullWritable, Object> {

    private FileSplit fileSplit;
    private Configuration conf;

    private ObjectReader reader;

    private Object current;
    private MappingIterator<Object> iterator;

    @Override public void initialize(
            InputSplit split, TaskAttemptContext context
    ) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();

        try {

            Configuration configuration = context.getConfiguration();
            String className = configuration.get(CSV_INPUT_FORMAT_BEAN_CLASS);
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);

            CsvMapper mapper = new CsvMapper();

            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            reader = mapper.readerFor(clazz).with(schema);

            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = fs.open(file);
            iterator = reader.readValues((InputStream) in);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (iterator.hasNext()) {
            current = iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return current;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }
}
