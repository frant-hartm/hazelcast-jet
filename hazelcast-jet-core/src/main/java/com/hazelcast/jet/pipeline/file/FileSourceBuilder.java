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
import com.hazelcast.jet.pipeline.file.impl.FileSourceFactory;
import com.hazelcast.jet.pipeline.file.impl.LocalFileSourceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * A unified builder object for various kinds of file sources. It works
 * with the local filesystem and several distributed filesystems
 * supported through the Hadoop API.
 * <p>
 * The builder requires two parameters: {@code path} and {@code format},
 * and creates a {@link BatchSource}. The path specifies the filesystem
 * type (examples: {@code s3a://}, {@code hdfs://}) and the path to the
 * file.
 * <p>
 * The format determines how Jet will map the contents of the file to the
 * objects coming out of the corresponding pipeline source. This involves
 * two major concerns: parsing and deserialization. For example, {@link
 * LinesTextFileFormat} parses the file into lines of text and emits a
 * simple string for each line; {@link JsonFileFormat} parses a JSON Lines
 * file and emits instances of the class you specify.
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
        HADOOP_PREFIXES = Collections.unmodifiableList(asList(
                "s3a://",   // Amazon S3
                "hdfs://",  // HDFS
                "wasbs://", // Azure Cloud Storage
                "adl://",   // Azure Data Lake
                "gs://"     // Google Cloud Storage
        ));
    }

    private final Map<String, String> options = new HashMap<>();

    private final String path;
    private FileFormat<T> format;
    private boolean useHadoop;

    /**
     * Creates a new file source builder with the given path. It can point
     * to a file, a directory or contain a glob (e.g. {@code "file*"},
     * capturing {@code file1.txt}, {@code file2.txt}, ...).
     *
     * @param path the path to the file
     */
    public FileSourceBuilder(@Nonnull String path) {
        this.path = requireNonNull(path, "path must not be null");
    }

    /**
     * Set the file format for the source. Currently supported file formats are:
     * <ul>
     * <li> {@link AvroFileFormat}
     * <li> {@link CsvFileFormat}
     * <li> {@link JsonFileFormat}
     * <li> {@link LinesTextFileFormat}
     * <li> {@link ParquetFileFormat}
     * <li> {@link RawBytesFileFormat}
     * <li> {@link TextFileFormat}
     * </ul>
     * You may provide a custom format by implementing the {@link FileFormat}
     * interface. See its javadoc for details.
     */
    @Nonnull
    public <T_NEW> FileSourceBuilder<T_NEW> withFormat(@Nonnull FileFormat<T_NEW> fileFormat) {
        @SuppressWarnings("unchecked")
        FileSourceBuilder<T_NEW> newThis = (FileSourceBuilder<T_NEW>) this;
        newThis.format = fileFormat;
        return newThis;
    }

    /**
     * Specifies to use Hadoop for files from local filesystem. One advantage
     * of Hadoop is that it can provide better parallelization when the number
     * of files is smaller than the total parallelism of the pipeline source.
     */
    @Nonnull
    public FileSourceBuilder<T> useHadoopForLocalFiles() {
        useHadoop = true;
        return this;
    }

    /**
     * Specifies an arbitrary option for the underlying source. If you are
     * looking for a missing option, check out the {@link FileFormat} class
     * you're using, it offers parsing-related options.
     */
    @Nonnull
    public FileSourceBuilder<T> withOption(String key, String value) {
        requireNonNull(key, "key must not be null");
        requireNonNull(value, "value must not be null");
        options.put(key, value);
        return this;
    }

    /**
     * Returns the options configured for the file source.
     */
    @Nonnull
    public Map<String, String> options() {
        return options;
    }

    /**
     * Returns the source path.
     */
    @Nonnull
    public String path() {
        return path;
    }

    /**
     * Returns the source format.
     */
    @Nullable
    public FileFormat<T> format() {
        return format;
    }

    /**
     * Builds a {@link BatchSource} based on the current state of the builder.
     */
    @Nonnull
    public BatchSource<T> build() {
        if (path == null) {
            throw new IllegalStateException("Parameter 'path' is required");
        }
        if (format == null) {
            throw new IllegalStateException("Parameter 'format' is required");
        }
        if (useHadoop || hasHadoopPrefix()) {
            ServiceLoader<FileSourceFactory> loader = ServiceLoader.load(FileSourceFactory.class);
            for (FileSourceFactory fileSourceFactory : loader) {
                return fileSourceFactory.create(this);
            }
            throw new JetException("No suitable FileSourceFactory found. " +
                    "Do you have Jet's Hadoop module on classpath?");
        }
        return new LocalFileSourceFactory().create(this);
    }

    private boolean hasHadoopPrefix() {
        long count = HADOOP_PREFIXES.stream().filter(path::startsWith).count();
        return count > 0;
    }
}