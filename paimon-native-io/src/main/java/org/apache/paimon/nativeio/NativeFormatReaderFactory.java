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

package org.apache.paimon.nativeio;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Map;

/** Creates native file readers through the normal format reader path. */
public class NativeFormatReaderFactory implements FormatReaderFactory {

    private final RowType readRowType;
    private final long fileRowCount;
    private final int batchSize;
    private final long maxBatchBytes;
    private final Map<String, String> objectStoreOptions;

    public NativeFormatReaderFactory(
            RowType readRowType,
            long fileRowCount,
            int batchSize,
            long maxBatchBytes,
            Map<String, String> objectStoreOptions) {
        this.readRowType = readRowType;
        this.fileRowCount = fileRowCount;
        this.batchSize = batchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.objectStoreOptions = objectStoreOptions;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        return new NativeFileRecordReader(
                context.filePath().toString(),
                readRowType,
                fileRowCount,
                batchSize,
                maxBatchBytes,
                objectStoreOptions);
    }
}
