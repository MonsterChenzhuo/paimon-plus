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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end test for native IO on deletion-vector primary-key tables. */
public class NativeIODvPrimaryKeyTableE2ETest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCreateWriteReadDvPrimaryKeyTableWithNativeIO() throws Exception {
        PaimonJnrLoader loader = PaimonJnrLoader.current();
        Assumptions.assumeTrue(
                loader.available(),
                () ->
                        "native IO library is not available: "
                                + loader.loadFailure()
                                        .map(Throwable::toString)
                                        .orElse(loader.resourcePath()));

        FileStoreTable table = createTable();
        writeCommit(
                table,
                1,
                GenericRow.of(1, 1, 100),
                GenericRow.of(1, 2, 200),
                GenericRow.of(1, 3, 300));
        writeCommit(
                table,
                2,
                GenericRow.ofKind(RowKind.DELETE, 1, 1, 100),
                GenericRow.of(1, 2, 220),
                GenericRow.of(1, 4, 400));

        FileStoreTable nativeTable = withNativeIOReadOptions(table);
        ReadBuilder readBuilder = nativeTable.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(split -> assertThat(((DataSplit) split).rawConvertible()).isTrue());

        List<String> rows = readRows(readBuilder);
        assertThat(rows).containsExactly("1,2,220", "1,3,300", "1,4,400");
    }

    private FileStoreTable createTable() throws Exception {
        CatalogContext context = CatalogContext.create(new Path(tempDir.toUri().toString()));
        Catalog catalog = CatalogFactory.createCatalog(context);
        Identifier identifier = Identifier.create("default", "native_dv_pk");
        catalog.createDatabase(identifier.getDatabaseName(), true);

        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build();
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private void writeCommit(FileStoreTable table, long commitIdentifier, GenericRow... rows)
            throws Exception {
        StreamTableWrite write = table.newStreamWriteBuilder().newWrite();
        write.withIOManager(new IOManagerImpl(tempDir.toString()));
        StreamTableCommit commit = table.newStreamWriteBuilder().newCommit();
        try {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
        } finally {
            write.close();
            commit.close();
        }
    }

    private FileStoreTable withNativeIOReadOptions(FileStoreTable table) {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.NATIVE_IO_ENABLED.key(), "true");
        dynamicOptions.put(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key(), "spark");
        dynamicOptions.put(CoreOptions.NATIVE_IO_BATCH_SIZE.key(), "2");
        return table.copy(dynamicOptions);
    }

    private List<String> readRows(ReadBuilder readBuilder) throws Exception {
        List<String> rows = new ArrayList<>();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        reader.forEachRemaining(
                row -> rows.add(row.getInt(0) + "," + row.getInt(1) + "," + row.getInt(2)));
        rows.sort(String::compareTo);
        return rows;
    }
}
