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

import org.apache.paimon.operation.nativeio.diagnostics.NativeIODiagnosticsEmitter;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEventType;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOPhase;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/** Aggregates Java-side native reader timing and emits Native IO diagnostics. */
public final class NativeReadDiagnostics {

    private final String operationId;
    private final String operationName;
    private final String filePath;
    private final Consumer<NativeIOEvent> sink;
    private final Map<NativeIOPhase, PhaseStats> phaseStats = new EnumMap<>(NativeIOPhase.class);

    private boolean started;
    private boolean ended;
    private long rows;
    private long bytes;
    private long batches;
    private long emptyBatches;

    private NativeReadDiagnostics(
            String operationId,
            String operationName,
            String filePath,
            Consumer<NativeIOEvent> sink) {
        this.operationId = operationId;
        this.operationName = operationName;
        this.filePath = filePath;
        this.sink = sink;
    }

    public static NativeReadDiagnostics create(String operationName, String filePath) {
        return create(operationName, filePath, NativeIODiagnosticsEmitter::emit);
    }

    static NativeReadDiagnostics createForTesting(
            String operationName, String filePath, Consumer<NativeIOEvent> sink) {
        return create(operationName, filePath, sink);
    }

    static NativeReadDiagnostics noop(String operationName, String filePath) {
        return create(operationName, filePath, event -> {});
    }

    private static NativeReadDiagnostics create(
            String operationName, String filePath, Consumer<NativeIOEvent> sink) {
        return new NativeReadDiagnostics(
                operationName + "-" + UUID.randomUUID(), operationName, filePath, sink);
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;
        emit(NativeIOEventType.OPERATION_START, null, null, null, null, null);
    }

    public PhaseTimer startPhase(NativeIOPhase phase) {
        if (ended) {
            return PhaseTimer.noop();
        }
        start();
        emit(NativeIOEventType.PHASE_START, phase, null, null, null, null);
        return new PhaseTimer(this, phase, System.nanoTime());
    }

    void recordPhaseForTesting(
            NativeIOPhase phase, long durationNs, @Nullable Long rows, @Nullable Long bytes) {
        recordPhase(phase, durationNs, rows, bytes);
    }

    public void end() {
        if (ended) {
            return;
        }
        ended = true;
        emit(NativeIOEventType.OPERATION_END, null, null, rows, bytes, metricsJson());
    }

    public void fail(Throwable throwable) {
        if (ended) {
            return;
        }
        ended = true;
        NativeIOEvent.Builder builder =
                base(NativeIOEventType.ERROR, null)
                        .withErrorClass(throwable.getClass().getName())
                        .withErrorMessage(throwable.getMessage())
                        .withRows(rows)
                        .withBytes(bytes)
                        .withMetricsJson(metricsJson());
        sink.accept(builder.build());
    }

    private void recordPhase(
            NativeIOPhase phase, long durationNs, @Nullable Long rows, @Nullable Long bytes) {
        PhaseStats stats = phaseStats.get(phase);
        if (stats == null) {
            stats = new PhaseStats();
            phaseStats.put(phase, stats);
        }
        stats.count++;
        stats.durationNs += durationNs;
        if (rows != null) {
            stats.rows += rows;
            if (phase == NativeIOPhase.READ_BATCH) {
                if (rows > 0) {
                    this.batches++;
                } else {
                    this.emptyBatches++;
                }
                this.rows += rows;
            }
        }
        if (bytes != null) {
            stats.bytes += bytes;
            if (phase == NativeIOPhase.READ_BATCH) {
                this.bytes += bytes;
            }
        }
        emit(NativeIOEventType.PHASE_END, phase, durationNsToMs(durationNs), rows, bytes, null);
    }

    private void emit(
            NativeIOEventType eventType,
            @Nullable NativeIOPhase phase,
            @Nullable Long durationMs,
            @Nullable Long rows,
            @Nullable Long bytes,
            @Nullable String metricsJson) {
        sink.accept(
                base(eventType, phase)
                        .withDurationMs(durationMs)
                        .withRows(rows)
                        .withBytes(bytes)
                        .withMetricsJson(metricsJson)
                        .build());
    }

    private NativeIOEvent.Builder base(NativeIOEventType eventType, @Nullable NativeIOPhase phase) {
        return NativeIOEvent.builder(eventType, operationId, operationName)
                .withPhase(phase)
                .withFilePath(filePath)
                .withThreadId(Thread.currentThread().getId());
    }

    private String metricsJson() {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        boolean[] first = new boolean[] {true};
        appendNumber(builder, first, "rows", rows);
        appendNumber(builder, first, "bytes", bytes);
        appendNumber(builder, first, "batches", batches);
        appendNumber(builder, first, "empty_batches", emptyBatches);
        for (Map.Entry<NativeIOPhase, PhaseStats> entry : phaseStats.entrySet()) {
            String phase = phaseKey(entry.getKey());
            PhaseStats stats = entry.getValue();
            appendNumber(builder, first, phase + "_ms", durationNsToMs(stats.durationNs));
            appendNumber(builder, first, phase + "_count", stats.count);
            if (stats.rows > 0) {
                appendNumber(builder, first, phase + "_rows", stats.rows);
            }
            if (stats.bytes > 0) {
                appendNumber(builder, first, phase + "_bytes", stats.bytes);
            }
        }
        builder.append('}');
        return builder.toString();
    }

    private static void appendNumber(
            StringBuilder builder, boolean[] first, String name, long value) {
        if (!first[0]) {
            builder.append(',');
        }
        first[0] = false;
        appendJsonString(builder, name);
        builder.append(':').append(value);
    }

    private static void appendJsonString(StringBuilder builder, String value) {
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                builder.append('\\');
            }
            builder.append(c);
        }
        builder.append('"');
    }

    private static String phaseKey(NativeIOPhase phase) {
        return phase.name().toLowerCase(Locale.ROOT);
    }

    private static long durationNsToMs(long durationNs) {
        return durationNs / 1_000_000L;
    }

    public static long estimateBatchBytes(VectorSchemaRoot root) {
        long bytes = 0L;
        int rowCount = root.getRowCount();
        for (ValueVector vector : root.getFieldVectors()) {
            bytes = saturatedAdd(bytes, vector.getBufferSizeFor(rowCount));
        }
        return bytes;
    }

    private static long saturatedAdd(long left, int right) {
        long result = left + right;
        return result < left ? Long.MAX_VALUE : result;
    }

    /** Timer for one native read phase. */
    public static final class PhaseTimer implements AutoCloseable {

        @Nullable private final NativeReadDiagnostics diagnostics;
        @Nullable private final NativeIOPhase phase;
        private final long startNs;
        private boolean closed;

        private static PhaseTimer noop() {
            return new PhaseTimer(null, null, 0L);
        }

        private PhaseTimer(
                @Nullable NativeReadDiagnostics diagnostics,
                @Nullable NativeIOPhase phase,
                long startNs) {
            this.diagnostics = diagnostics;
            this.phase = phase;
            this.startNs = startNs;
        }

        public void close(@Nullable Long rows, @Nullable Long bytes) {
            if (closed) {
                return;
            }
            closed = true;
            if (diagnostics == null || phase == null) {
                return;
            }
            diagnostics.recordPhase(phase, System.nanoTime() - startNs, rows, bytes);
        }

        @Override
        public void close() {
            close(null, null);
        }
    }

    /** Aggregated metrics for one native read phase. */
    private static class PhaseStats {

        private long durationNs;
        private long count;
        private long rows;
        private long bytes;
    }
}
