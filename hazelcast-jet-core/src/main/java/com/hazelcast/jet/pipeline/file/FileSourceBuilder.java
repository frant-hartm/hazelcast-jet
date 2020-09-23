/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.impl.LocalFileSourceFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;

/**
 * Builder for file sources
 * <p>
 * The builder works with local filesystem and with hadoop supported
 * filesystems.
 * <p>
 * The builder requires 'path' and 'format' parameters and creates a
 * {@link BatchSource}. The path specifies the location of the file(s)
 * and possibly the data source - s3a://, hdfs://, etc..
 * <p>
 * The format determines how the contents of the file is parsed and
 * also determines the type of the source items. E.g. the
 * {@link LinesTextFileFormat} returns each line as a String,
 * {@link JsonFileFormat} returns each line of a JSON Lines file
 * deserialized into an instance of a specified class.
 * <p>
 * You may also use Hadoop to read local files by specifying the
 * {@link #useHadoopForLocalFiles()} flag.
 * <p>
 * Usage:
 * <pre>{@code
 * BatchSource<User> source = new FileSourceBuilder("data/users.jsonl")
 *   .withFormat(new JsonFileFormat<>(User.class))
 *   .build();
 * }</pre>
 *
 * @param <T>
 */
public class FileSourceBuilder<T> {

    private static final List<String> HADOOP_PREFIXES;

    static {
        ArrayList<String> list = new ArrayList<>();
        // Amazon S3
        list.add("s3a://");
        // HDFS
        list.add("hdfs://");
        // Azure Cloud Storage
        list.add("wasbs://");
        // Azure Data LAke
        list.add("adl://");
        // Google Cloud Storage
        list.add("gs://");
        HADOOP_PREFIXES = Collections.unmodifiableList(list);
    }

    private final Map<String, String> options = new HashMap<>();

    private final String path;
    private FileFormat<T> format;
    private boolean useHadoop;

    /**
     * Create a new builder with given path.
     * <p>
     * Path can point to a file, a directory or contain a glob
     * (e.g. 'file*' capturing file1, file2, ...).
     *
     * @param path path
     */
    public FileSourceBuilder(String path) {
        this.path = requireNonNull(path, "path must not be null");
    }

    /**
     * Set the file format for the source
     * <p>
     * Currently supported file formats are:
     * <li> {@link AvroFileFormat}
     * <li> {@link CsvFileFormat}
     * <li> {@link JsonFileFormat}
     * <li> {@link LinesTextFileFormat}
     * <li> {@link ParquetFileFormat}
     * <li> {@link RawBytesFileFormat}
     * <li> {@link TextFileFormat}
     * <p>
     * You may provide a custom format by implementing the
     * {@link FileFormat} interface. See its javadoc for details.
     */
    public <T_NEW> FileSourceBuilder<T_NEW> withFormat(FileFormat<T_NEW> fileFormat) {
        @SuppressWarnings("unchecked")
        FileSourceBuilder<T_NEW> newTHis = (FileSourceBuilder<T_NEW>) this;
        newTHis.format = fileFormat;
        return newTHis;
    }

    /**
     * Use hadoop for files from local filesystem
     * <p>
     * Using Hadoop may be advantageous when working with small number
     * of large files (smaller than total parallelism), because of
     * better parallelization.
     */
    public FileSourceBuilder<T> useHadoopForLocalFiles() {
        useHadoop = true;
        return this;
    }

    /**
     * Specify an option for the underlying source
     * <p>
     * NOTE: Format related options are set on the FileFormat directly.
     */
    public FileSourceBuilder<T> withOption(String key, String value) {
        requireNonNull(key, "key must not be null");
        requireNonNull(value, "value must not be null");
        options.put(key, value);
        return this;
    }

    /**
     * The configured source options
     */
    public Map<String, String> options() {
        return options;
    }

    /**
     * The source path
     */
    public String path() {
        return path;
    }

    /**
     * The source format
     */
    public FileFormat<T> format() {
        return format;
    }

    /**
     * Build a batch source based on the configuration
     */
    public BatchSource<T> build() {
        if (path == null) {
            throw new IllegalStateException("Parameter 'path' is required");
        }
        if (format == null) {
            throw new IllegalStateException("Parameter 'format' is required");
        }

        if (useHadoop || hasHadoopPrefix()) {

            ServiceLoader<FileSourceFactory> loader = ServiceLoader.load(FileSourceFactory.class);
            for (FileSourceFactory<T> fileSourceFactory : loader) {
                return fileSourceFactory.create(this);
            }

            throw new JetException("No suitable FileSourceFactory found. " +
                    "Do you have Jet's Hadoop module on classpath?");
        }
        
        return new LocalFileSourceFactory<T>().create(this);
    }

    private boolean hasHadoopPrefix() {
        long count = HADOOP_PREFIXES.stream().filter(path::startsWith).count();
        return count > 0;
    }
}
