/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.ParquetWriterFactory;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.org.apache.parquet.ParquetReadOptions;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for export parquet row-group copy compaction. */
class ExportParquetProcedureCopyCompactTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {new IntType(), new VarCharType()}, new String[] {"id", "name"});

    @TempDir java.nio.file.Path tempDir;

    @Test
    void copyParquetRowGroupsPreservesSourceRowGroups() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path first = new Path(tempDir.resolve("part-a.parquet").toUri());
        Path second = new Path(tempDir.resolve("part-b.parquet").toUri());
        writeParquet(fileIO, first, 1, "a");
        writeParquet(fileIO, second, 2, "b");

        Path compacted = new Path(tempDir.resolve("part-compact.parquet").toUri());
        ExportParquetProcedure.copyParquetRowGroups(
                fileIO,
                Arrays.asList(fileIO.getFileStatus(first), fileIO.getFileStatus(second)),
                compacted);

        try (ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(
                                fileIO, compacted, fileIO.getFileStatus(compacted).getLen()),
                        ParquetReadOptions.builder().build(),
                        null)) {
            assertThat(reader.getFooter().getBlocks()).hasSize(2);
            assertThat(
                            reader.getFooter().getBlocks().stream()
                                    .mapToLong(BlockMetaData::getRowCount)
                                    .sum())
                    .isEqualTo(2L);
        }
    }

    @Test
    void parquetFileGroupsUseTargetFileSize() {
        List<List<FileStatus>> groups =
                ExportParquetProcedure.parquetFileGroups(
                        Arrays.asList(
                                fileStatus("part-a.parquet", 40L),
                                fileStatus("part-b.parquet", 40L),
                                fileStatus("part-c.parquet", 40L)),
                        100L,
                        10);

        assertThat(groups).hasSize(2);
        assertThat(groups.get(0)).hasSize(2);
        assertThat(groups.get(1)).hasSize(1);
    }

    @Test
    void parquetFileGroupsLimitEstimatedColumnChunksForWideTables() {
        FileStatus[] files = new FileStatus[12];
        for (int i = 0; i < files.length; i++) {
            files[i] = fileStatus("part-" + i + ".parquet", 1L);
        }

        List<List<FileStatus>> groups =
                ExportParquetProcedure.parquetFileGroups(
                        Arrays.asList(files), Long.MAX_VALUE, 20_000);

        assertThat(groups).hasSize(3);
        assertThat(groups.get(0)).hasSize(5);
        assertThat(groups.get(1)).hasSize(5);
        assertThat(groups.get(2)).hasSize(2);
    }

    private static void writeParquet(FileIO fileIO, Path path, int id, String name)
            throws Exception {
        FormatWriter writer =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, new Options()))
                        .create(fileIO.newOutputStream(path, false), "snappy");
        try {
            writer.addElement(GenericRow.of(id, BinaryString.fromString(name)));
        } finally {
            writer.close();
        }
    }

    private static FileStatus fileStatus(String name, long length) {
        return new FileStatus() {
            @Override
            public long getLen() {
                return length;
            }

            @Override
            public boolean isDir() {
                return false;
            }

            @Override
            public Path getPath() {
                return new Path("file:///" + name);
            }

            @Override
            public long getModificationTime() {
                return 0L;
            }
        };
    }
}
