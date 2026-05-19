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

package org.apache.paimon.operation.nativeio;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SupportsNativeIOTest {

    @Test
    void rejectsNonSparkEngine() {
        Options raw = new Options();
        raw.setString("native-io.enabled", "true");
        NativeApplicability result = SupportsNativeIO.checkEngine(NativeIOOptions.from(raw));

        assertThat(result.applicable()).isFalse();
        assertThat(result.reason()).isEqualTo(NativeRejectReason.UNSUPPORTED_ENGINE);
    }

    @Test
    void rejectsUnsupportedTypeByDefault() {
        RowType rowType = RowType.of(new MapType(new VarCharType(), new IntType()));

        assertThat(SupportsNativeIO.hasUnsupportedTypes(rowType)).isTrue();
    }

    @Test
    void rejectsTimestampWithLocalTimezone() {
        RowType rowType = RowType.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());

        assertThat(SupportsNativeIO.hasUnsupportedTypes(rowType)).isTrue();
    }

    @Test
    void rejectsTimestampPrecisionAboveSix() {
        assertThat(SupportsNativeIO.hasUnsupportedTypes(RowType.of(DataTypes.TIMESTAMP(6))))
                .isFalse();
        assertThat(SupportsNativeIO.hasUnsupportedTypes(RowType.of(DataTypes.TIMESTAMP(9))))
                .isTrue();
    }

    @Test
    void rejectsNativeFileWithoutObsPathOrConfig() {
        DataFileMeta file = DataFileTestUtils.newFile("data.parquet", 0, 1, 1, 1, 0L);
        Options raw = enabledSparkOptions();

        NativeApplicability localPath =
                SupportsNativeIO.checkNativeFile(
                        file,
                        null,
                        RowType.of(new IntType()),
                        false,
                        NativeIOOptions.from(raw),
                        new Path("file:/tmp/data.parquet"));
        assertThat(localPath.applicable()).isFalse();
        assertThat(localPath.reason()).isEqualTo(NativeRejectReason.NON_OBS_PATH);

        NativeApplicability missingConfig =
                SupportsNativeIO.checkNativeFile(
                        file,
                        null,
                        RowType.of(new IntType()),
                        false,
                        NativeIOOptions.from(raw),
                        new Path("obs://bucket/data.parquet"));
        assertThat(missingConfig.applicable()).isFalse();
        assertThat(missingConfig.reason()).isEqualTo(NativeRejectReason.MISSING_OBS_CONFIG);
    }

    @Test
    void rejectsNativeFileObsPathWithQueryOrFragment() {
        DataFileMeta file = DataFileTestUtils.newFile("data.parquet", 0, 1, 1, 1, 0L);
        Options raw = enabledSparkObsOptions();

        NativeApplicability queryPath =
                SupportsNativeIO.checkNativeFile(
                        file,
                        null,
                        RowType.of(new IntType()),
                        false,
                        NativeIOOptions.from(raw),
                        new Path("obs://bucket/data.parquet?version=1"));
        assertThat(queryPath.applicable()).isFalse();
        assertThat(queryPath.reason()).isEqualTo(NativeRejectReason.NON_OBS_PATH);

        NativeApplicability fragmentPath =
                SupportsNativeIO.checkNativeFile(
                        file,
                        null,
                        RowType.of(new IntType()),
                        false,
                        NativeIOOptions.from(raw),
                        new Path("obs://bucket/data.parquet#fragment"));
        assertThat(fragmentPath.applicable()).isFalse();
        assertThat(fragmentPath.reason()).isEqualTo(NativeRejectReason.NON_OBS_PATH);

        NativeApplicability encodedPath =
                SupportsNativeIO.checkNativeFile(
                        file,
                        null,
                        RowType.of(new IntType()),
                        false,
                        NativeIOOptions.from(raw),
                        new Path("obs://bucket/data%3Fv%3D1.parquet"));
        assertThat(encodedPath.applicable()).isTrue();
    }

    @Test
    void rejectsNativeFileWhenRowTrackingEnabled() {
        Options raw = enabledSparkOptions();
        raw.setString("fs.obs.endpoint", "obs.example.com");
        raw.setString("fs.obs.access.key", "ak");
        raw.setString("fs.obs.secret.key", "sk");

        NativeApplicability result =
                SupportsNativeIO.checkNativeFile(
                        DataFileTestUtils.newFile("data.parquet", 0, 1, 1, 1, 0L),
                        null,
                        RowType.of(new IntType()),
                        true,
                        NativeIOOptions.from(raw),
                        new Path("obs://bucket/data.parquet"));

        assertThat(result.applicable()).isFalse();
        assertThat(result.reason()).isEqualTo(NativeRejectReason.PARTITION_OR_SYSTEM_FIELDS);
    }

    private static Options enabledSparkOptions() {
        Options raw = new Options();
        raw.setString("native-io.enabled", "true");
        raw.setString("__paimon.internal.native-io.engine", "spark");
        return raw;
    }

    private static Options enabledSparkObsOptions() {
        Options raw = enabledSparkOptions();
        raw.setString("fs.obs.endpoint", "obs.example.com");
        raw.setString("fs.obs.access.key", "ak");
        raw.setString("fs.obs.secret.key", "sk");
        return raw;
    }
}
