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

package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

public class CsvFileFormatTest extends BaseFileFormatTest {

    @Test
    public void shouldReadCsvFile() throws Exception {

        FileSourceBuilder<User> source = FileSources.files("src/test/resources/file.csv")
                                                    .withFormat(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }
}
