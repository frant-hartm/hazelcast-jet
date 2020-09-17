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

package com.hazelcast.jet.avro;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.impl.MapFnProvider;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * MapFnProvider for Avro files, reading given path and deserializing using avro DatumReader
 *
 * @param <T> type of the item emitted from the source
 */
public class AvroMapFnProvider<T> implements MapFnProvider<AvroFileFormat<T>, T> {

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

    @Override
    public String format() {
        return "avro";
    }
}
