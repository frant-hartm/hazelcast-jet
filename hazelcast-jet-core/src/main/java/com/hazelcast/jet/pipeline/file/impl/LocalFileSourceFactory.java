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

package com.hazelcast.jet.pipeline.file.impl;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implementation of FileSourceFactory for local filesystem
 *
 * @param <T> type of the item emitted from the source
 */
public class LocalFileSourceFactory<T> implements FileSourceFactory<T> {

    private Map<Class<? extends FileFormat>, MapFnProvider<? extends FileFormat<?>, ?>> mapFns;

    /**
     * Default constructor
     */
    public LocalFileSourceFactory() {
        mapFns = new HashMap<>();

        mapFns.put(AvroFileFormat.class, new AvroMapFnProvider<>());
        mapFns.put(CsvFileFormat.class, new CsvMapFnProvider());
        mapFns.put(JsonFileFormat.class, new JsonMapFnProvider<>());
        mapFns.put(LinesTextFileFormat.class, new LineMapFnProvider());
        mapFns.put(ParquetFileFormat.class, new ParquetMapFnProvider());
        mapFns.put(RawBytesFileFormat.class, new RawBytesMapFnProvider());
        mapFns.put(TextFileFormat.class, new TextMapFnProvider());
    }

    @Override
    public BatchSource<T> create(FileSourceBuilder<T> builder) {
        Path p = Paths.get(builder.path());

        String directory;
        String glob = "*";
        if (p.toFile().isDirectory()) {
            directory = p.toString();
        } else {
            Path parent = p.getParent();
            if (parent != null) {
                directory = parent.toString();

                Path fileName = p.getFileName();
                if (fileName != null) {
                    glob = fileName.toString();
                } else {
                    glob = "*";
                }
            } else {
                // p is root of the filesystem, has no parent
                directory = p.toString();
            }
        }

        FileFormat<?> format = builder.format();
        MapFnProvider<FileFormat<?>, T> mapFnProvider = (MapFnProvider<FileFormat<?>, T>) mapFns.get(format.getClass());
        FunctionEx<Path, Stream<T>> mapFn = mapFnProvider.create(format);
        return Sources.filesBuilder(directory)
                      .glob(glob)
                      .build(mapFn);
    }

    private interface MapFnProvider<F extends FileFormat<?>, T> {

        FunctionEx<Path, Stream<T>> create(F format);

    }

    private static class AvroMapFnProvider<T> implements MapFnProvider<AvroFileFormat<T>, T> {

        @Override
        public FunctionEx<Path, Stream<T>> create(AvroFileFormat<T> format) {
            Class<T> reflectClass = format.reflectClass();
            return (path) -> {
                DatumReader<T> datumReader = datumReader(reflectClass);
                DataFileReader<T> reader = new DataFileReader<>(path.toFile(), datumReader);
                return StreamSupport.stream(reader.spliterator(), false)
                                    .onClose(() -> uncheckRun(reader::close));
            };
        }

        private static <T> DatumReader<T> datumReader(Class<T> reflectClass) {
            return reflectClass == null ? new SpecificDatumReader<>() : new ReflectDatumReader<>(reflectClass);
        }
    }

    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    private abstract static class AbstractMapFnProvider<F extends FileFormat<?>, T> implements MapFnProvider<F, T> {

        public FunctionEx<Path, Stream<T>> create(F format) {
            FunctionEx<InputStream, Stream<T>> mapInputStreamFn = mapInputStreamFn(format);
            return path -> {
                FileInputStream fis = new FileInputStream(path.toFile());
                return mapInputStreamFn.apply(fis).onClose(() -> uncheckRun(fis::close));
            };
        }

        abstract FunctionEx<InputStream, Stream<T>> mapInputStreamFn(F format);
    }

    private static class CsvMapFnProvider extends AbstractMapFnProvider<CsvFileFormat<?>, Object> {

        @Override
        FunctionEx<InputStream, Stream<Object>> mapInputStreamFn(CsvFileFormat<?> format) {
            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            CsvMapper mapper = new CsvMapper();
            ObjectReader reader = mapper.readerFor(format.clazz()).with(schema);
            return is -> StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                            reader.readValues(is),
                            Spliterator.ORDERED),
                    false);
        }
    }

    private static class JsonMapFnProvider<T> extends AbstractMapFnProvider<JsonFileFormat<T>, T> {

        @Override
        FunctionEx<InputStream, Stream<T>> mapInputStreamFn(JsonFileFormat<T> format) {
            Class<T> thisClazz = format.clazz();
            return is -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, UTF_8));

                return reader.lines()
                             .map(line -> uncheckCall(() -> JsonUtil.beanFrom(line, thisClazz)))
                             .onClose(() -> uncheckRun(reader::close));
            };
        }
    }

    private static class LineMapFnProvider extends AbstractMapFnProvider<LinesTextFileFormat, String> {

        @Override FunctionEx<InputStream, Stream<String>> mapInputStreamFn(LinesTextFileFormat format) {
            String thisCharset = format.charset().name();
            return is -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));
                return reader.lines()
                             .onClose(() -> uncheckRun(reader::close));
            };
        }
    }

    private static class ParquetMapFnProvider implements MapFnProvider<ParquetFileFormat<?>, Object> {

        @Override
        public FunctionEx<Path, Stream<Object>> create(ParquetFileFormat<?> format) {
            throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode." +
                    " " +
                    "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
        }
    }

    private static class RawBytesMapFnProvider extends AbstractMapFnProvider<RawBytesFileFormat, Object> {

        @Override
        FunctionEx<InputStream, Stream<Object>> mapInputStreamFn(RawBytesFileFormat format) {
            return is -> Stream.of(IOUtil.readFully(is));
        }
    }

    private static class TextMapFnProvider extends AbstractMapFnProvider<TextFileFormat, Object> {

        @Override
        FunctionEx<InputStream, Stream<Object>> mapInputStreamFn(TextFileFormat format) {
            String thisCharset = format.charset().name();
            return is -> Stream.of(new String(IOUtil.readFully(is), Charset.forName(thisCharset)));
        }
    }
}
