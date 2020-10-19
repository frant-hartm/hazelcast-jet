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

package com.hazelcast.jet.sql.impl.inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class JsonUpsertTarget implements UpsertTarget {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ObjectNode json;

    JsonUpsertTarget() {
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(String path, QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return value -> json.put(path, value == null ? null : (boolean) value);
            case TINYINT:
                return value -> {
                    if (value == null) {
                        json.putNull(path);
                    } else {
                        json.put(path, (byte) value);
                    }
                };
            case SMALLINT:
                return value -> json.put(path, value == null ? null : (short) value);
            case INTEGER:
                return value -> json.put(path, value == null ? null : (int) value);
            case BIGINT:
                return value -> json.put(path, value == null ? null : (long) value);
            case REAL:
                return value -> json.put(path, value == null ? null : (float) value);
            case DOUBLE:
                return value -> json.put(path, value == null ? null : (double) value);
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return value -> json.put(path, (String) QueryDataType.VARCHAR.convert(value));
            default:
                return value -> {
                    if (value == null) {
                        json.putNull(path);
                    } else if (value instanceof JsonNode) {
                        json.set(path, (JsonNode) value);
                    } else {
                        throw QueryException.error("Cannot set field \"" + path + "\" of type " + type);
                    }
                };
        }
    }

    @Override
    public void init() {
        json = MAPPER.createObjectNode();
    }

    @Override
    public Object conclude() {
        ObjectNode json = this.json;
        this.json = null;
        return json;
    }
}
