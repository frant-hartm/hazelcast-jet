package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.hadoop.file.generated.SpecificUser;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;

public class ParquetFileTest extends BaseFileTest {

    // Parquet had a dependency on Hadoop so it does not make sense to run it without it
    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true);
    }

    @Test
    public void shouldReadParquetFile() throws Exception {
        createParquetFile();

        FileSourceBuilder<SpecificUser> source = FileSources.files("target/parquet/file.parquet")
                                                            .withFormat(new ParquetFileFormat<>());
        assertItemsInSource(source,
                new SpecificUser("Frantisek", 7),
                new SpecificUser("Ali", 42)
        );
    }

    private void createParquetFile() throws IOException {
        Path inputPath = new Path("target/parquet");
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);
        Path filePath = new Path(inputPath, "file.parquet");

        ParquetWriter<SpecificUser> writer = AvroParquetWriter.
                <SpecificUser>builder(filePath)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(SpecificUser.SCHEMA$)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();

        writer.write(new SpecificUser("Frantisek", 7));
        writer.write(new SpecificUser("Ali", 42));
        writer.close();
        fs.close();
    }
}
