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

/**
 * FileFormat for avro files
 *
 * @param <T> type of items emitted from the source
 */
public class AvroFileFormat<T> implements FileFormat<T> {

    private Class<T> reflectClass;

    /**
     * Use reflection to deserialize data into given class
     * <p>
     * If set, uses ReflectDatumReader to read Avro data.
     *
     * @param reflectClass class to deserialize data into
     */
    public AvroFileFormat<T> withReflect(Class<T> reflectClass) {
        this.reflectClass = reflectClass;
        return this;
    }

    /**
     * Class to deserialize data into using reflection.
     *
     * Null if not set.
     */
    public Class<T> reflectClass() {
        return reflectClass;
    }

    @Override
    public String format() {
        return "avro";
    }
}
