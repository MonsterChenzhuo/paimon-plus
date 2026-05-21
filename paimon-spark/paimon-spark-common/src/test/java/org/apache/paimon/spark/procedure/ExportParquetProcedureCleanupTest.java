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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for export parquet output cleanup. */
class ExportParquetProcedureCleanupTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testDeleteDirectoryQuietlyRemovesNonEmptyDirectory() throws Exception {
        java.nio.file.Path backupDir = tempDir.resolve("backup");
        Files.createDirectories(backupDir);
        Files.write(backupDir.resolve("part-000.parquet"), new byte[] {1});

        FileIO fileIO = new LocalFileIO();
        Path path = new Path(backupDir.toUri());

        ExportParquetProcedure.deleteDirectoryQuietly(fileIO, path);

        assertThat(fileIO.exists(path)).isFalse();
    }
}
