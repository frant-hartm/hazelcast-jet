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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlPrimitiveTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_insertIntoDiscoveredMap() {
        String name = generateRandomName();

        instance().getMap(name).put(BigInteger.valueOf(1), "Alice");

        assertMapEventually(
                name,
                "INSERT OVERWRITE partitioned." + name + " VALUES (2, 'Bob')",
                createMap(BigInteger.valueOf(1), "Alice", BigInteger.valueOf(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(BigDecimal.valueOf(1), "Alice"),
                        new Row(BigDecimal.valueOf(2), "Bob")
                )
        );
    }

    @Test
    public void test_insertSelect() {
        String name = createTableWithRandomName();

        IMap<Integer, String> source = instance().getMap("source");
        source.put(0, "value-0");
        source.put(1, "value-1");

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " SELECT * FROM " + source.getName(),
                createMap(0, "value-0", 1, "value-1")
        );
    }

    @Test
    public void test_insertValues() {
        String name = createTableWithRandomName();

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (this, __key) VALUES ('2', 1)",
                createMap(1, "2")
        );
        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (this, __key) VALUES ('4', 3)",
                createMap(1, "2", 3, "4")
        );
    }

    @Test
    public void supportsInsertWithProject() {
        String name = createTableWithRandomName();

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (__key, this) VALUES (CAST(0 + 1 AS INT), 2)",
                createMap(1, "2")
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = generateRandomName();

        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key"
                + ", name VARCHAR EXTERNAL NAME this"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ")"
        );

        assertMapEventually(
                name,
                format("INSERT OVERWRITE %s (name, id) VALUES ('value-2', 2)", name),
                createMap(2, "value-2")
        );
    }

    @Test
    public void when_plainInsert_then_throws() {
        String name = createTableWithRandomName();

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO " + name + " (__key, this) VALUES (1, '2')"))
                .hasMessageContaining("Only INSERT OVERWRITE clause is supported for IMapSqlConnector");
    }

    private static String createTableWithRandomName() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));
        return name;
    }

    private static String generateRandomName() {
        return "primitive_" + randomString().replace('-', '_');
    }
}
