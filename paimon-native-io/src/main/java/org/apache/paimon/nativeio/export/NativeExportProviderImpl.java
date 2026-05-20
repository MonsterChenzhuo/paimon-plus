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

package org.apache.paimon.nativeio.export;

import org.apache.paimon.operation.nativeio.export.NativeExportContext;
import org.apache.paimon.operation.nativeio.export.NativeExportPlanDescriptor;
import org.apache.paimon.operation.nativeio.export.NativeExportPreflightResult;
import org.apache.paimon.operation.nativeio.export.NativeExportProvider;
import org.apache.paimon.operation.nativeio.export.NativeExportTaskResult;

/** Native export provider implementation discovered through core SPI. */
public final class NativeExportProviderImpl implements NativeExportProvider {

    private static final long serialVersionUID = 1L;

    @Override
    public NativeExportPreflightResult preflight(NativeExportContext context) {
        return new NativeExportPlanner().preflight(context);
    }

    @Override
    public NativeExportPlanDescriptor plan(NativeExportContext context) {
        return new NativeExportPlanner().plan(context);
    }

    @Override
    public NativeExportTaskResult executeTask(byte[] taskPayload) throws Exception {
        NativeExportTask task = NativeExportJson.taskFromPayload(taskPayload);
        NativeExportResult result = new NativeExportRunner().run(task);
        return new NativeExportTaskResult(
                result.rowsOutput(), NativeExportJson.metricsToJson(result.metrics()));
    }
}
