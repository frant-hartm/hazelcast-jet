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

import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

public class TextFileFormatTest extends BaseFileFormatTest {

    @Test
    public void readTextFileAsSingleItem() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/file.txt")
                                                      .withFormat(new TextFileFormat());

        assertItemsInSource(source, "Text contents of\nthe file.\n");
    }

    @Test
    public void glob() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/file*")
                                                      .withFormat(new TextFileFormat());

        assertItemsInSource(source, "file", "file1", "file*");
    }

    @Test
    public void escapedGlob() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/file\\*")
                                                      .withFormat(new TextFileFormat());

        assertItemsInSource(source, "file*");
    }

    @Test
    public void readTextFileAsLines() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/file.txt")
                                                      .withFormat(new LinesTextFileFormat());

        assertItemsInSource(source, "Text contents of", "the file.");
    }

    @Test
    public void shouldReadTextFileWithCharset() {
        // Charset ont available on Hadoop - all text is in UTF-8
        assumeThat(useHadoop, is(false));

        FileSourceBuilder<String> source = FileSources.files("src/test/resources/cp1250.txt")
                                                      .withFormat(new TextFileFormat(Charset.forName("Cp1250")));

        assertItemsInSource(source, "Příliš žluťoučký kůň úpěl ďábelské ódy.");
    }

    @Test
    public void shouldReadAllFilesInDirectory() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/directory/")
                                                      .withFormat(new TextFileFormat(Charset.forName("Cp1250")));

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void defaultFileFormatShouldReadFileAsLines() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/file.txt");

        assertItemsInSource(source, "Text contents of", "the file.");
    }
}
