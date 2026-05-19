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

package org.apache.paimon.io;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataFileRecordReaderTest {

    @Test
    void doesNotIgnoreNonCorruptReadExceptions() {
        IOException wrapper =
                new IOException(
                        "native reader failed",
                        new NonCorruptFileReadException("permission denied"));

        assertThat(DataFileRecordReader.ignoreCorruptException(wrapper, true)).isFalse();
    }

    @Test
    void doesNotTreatNonCorruptCreateReaderFailureAsLostFile() {
        Path file = new Path("obs://bucket/missing.parquet");
        FormatReaderFactory readerFactory =
                context -> {
                    throw new NonCorruptFileReadException("native unavailable");
                };

        assertThatThrownBy(
                        () ->
                                new DataFileRecordReader(
                                        RowType.of(),
                                        readerFactory,
                                        new TestFormatReaderContext(new MissingFileIO(), file),
                                        true,
                                        true,
                                        null,
                                        null,
                                        null,
                                        false,
                                        null,
                                        0L,
                                        Collections.emptyMap()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to create FileRecordReader")
                .hasCauseInstanceOf(NonCorruptFileReadException.class);
    }

    private static class TestFormatReaderContext implements FormatReaderFactory.Context {

        private final FileIO fileIO;
        private final Path filePath;

        private TestFormatReaderContext(FileIO fileIO, Path filePath) {
            this.fileIO = fileIO;
            this.filePath = filePath;
        }

        @Override
        public FileIO fileIO() {
            return fileIO;
        }

        @Override
        public Path filePath() {
            return filePath;
        }

        @Override
        public long fileSize() {
            return 0L;
        }

        @Override
        public RoaringBitmap32 selection() {
            return null;
        }
    }

    private static class MissingFileIO implements FileIO {

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        public void configure(CatalogContext context) {}

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            throw notUsed();
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            throw notUsed();
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            throw notUsed();
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            throw notUsed();
        }

        @Override
        public boolean exists(Path path) {
            return false;
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            throw notUsed();
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            throw notUsed();
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            throw notUsed();
        }

        private IOException notUsed() {
            return new IOException("not used");
        }
    }
}
