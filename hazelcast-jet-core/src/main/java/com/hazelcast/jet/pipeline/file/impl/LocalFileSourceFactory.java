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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implementation of FileSourceFactory for local filesystem
 *
 * @param <T> type of the item emitted from the source
 */
public class LocalFileSourceFactory<T> implements FileSourceFactory<T> {

    private Map<String, MapFnProvider<? extends FileFormat<?>, ?>> mapFns;

    /**
     * Default constructor
     */
    public LocalFileSourceFactory() {
        mapFns = new HashMap<>();

        mapFns.put("csv", new CsvMapFnProvider());
        mapFns.put("jsonl", new JsonMapFnProvider<>());
        mapFns.put("txtl", new LinesMapFnProvider());
        mapFns.put("parquet", new ParquetMapFnProvider());
        mapFns.put("bin", new RawBytesMapFnProvider());
        mapFns.put("txt", new TextMapFnProvider());

        ServiceLoader<MapFnProvider> loader = ServiceLoader.load(MapFnProvider.class);
        for (MapFnProvider mapFnProvider : loader) {
            mapFns.put(mapFnProvider.format(), mapFnProvider);
        }
    }

    @Override
    public BatchSource<T> create(FileSourceBuilder<T> builder) {
        Tuple2<String, String> dirAndGlob = deriveDirectoryAndGlobFromPath(builder.path());

        FileFormat<?> format = builder.format();
        MapFnProvider<FileFormat<?>, T> mapFnProvider = (MapFnProvider<FileFormat<?>, T>) mapFns.get(format.format());
        FunctionEx<Path, Stream<T>> mapFn = mapFnProvider.create(format);
        return Sources.filesBuilder(dirAndGlob.f0())
                      .glob(dirAndGlob.f1())
                      .build(mapFn);
    }

    private Tuple2<String, String> deriveDirectoryAndGlobFromPath(String path) {
        Path p = Paths.get(path);

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
        return tuple2(directory, glob);
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
            Class<?> formatClazz = format.clazz(); // Format is not Serializable
            return is -> {
                CsvSchema schema = CsvSchema.emptySchema().withHeader();
                CsvMapper mapper = new CsvMapper();
                ObjectReader reader = mapper.readerFor(formatClazz).with(schema);

                return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                reader.readValues(is),
                                Spliterator.ORDERED),
                        false);
            };
        }

        @Override
        public String format() {
            return "csv";
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

        @Override
        public String format() {
            return "jsonl";
        }
    }

    private static class LinesMapFnProvider extends AbstractMapFnProvider<LinesTextFileFormat, String> {

        @Override FunctionEx<InputStream, Stream<String>> mapInputStreamFn(LinesTextFileFormat format) {
            String thisCharset = format.charset().name();
            return is -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));
                return reader.lines()
                             .onClose(() -> uncheckRun(reader::close));
            };
        }

        @Override
        public String format() {
            return "txtl";
        }
    }

    private static class ParquetMapFnProvider implements MapFnProvider<ParquetFileFormat<?>, Object> {

        @Override
        public FunctionEx<Path, Stream<Object>> create(ParquetFileFormat<?> format) {
            throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode." +
                    " " +
                    "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
        }

        @Override
        public String format() {
            return "parquet";
        }
    }

    private static class RawBytesMapFnProvider extends AbstractMapFnProvider<RawBytesFileFormat, Object> {

        @Override
        FunctionEx<InputStream, Stream<Object>> mapInputStreamFn(RawBytesFileFormat format) {
            return is -> Stream.of(IOUtil.readFully(is));
        }

        @Override
        public String format() {
            return "bin";
        }
    }

    private static class TextMapFnProvider extends AbstractMapFnProvider<TextFileFormat, Object> {

        @Override
        FunctionEx<InputStream, Stream<Object>> mapInputStreamFn(TextFileFormat format) {
            String thisCharset = format.charset().name();
            return is -> Stream.of(new String(IOUtil.readFully(is), Charset.forName(thisCharset)));
        }

        @Override
        public String format() {
            return "txt";
        }
    }
}
