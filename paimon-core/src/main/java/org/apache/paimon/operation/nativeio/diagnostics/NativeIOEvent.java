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

package org.apache.paimon.operation.nativeio.diagnostics;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.UUID;

/** Versioned Native IO diagnostic event. */
public final class NativeIOEvent implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;

    private final int version;
    private final String eventId;
    private final long eventTime;
    private final NativeIOEventType eventType;
    private final String operationId;
    private final String operationName;
    @Nullable private final NativeIOPhase phase;
    @Nullable private final Long sqlExecutionId;
    @Nullable private final Integer stageId;
    @Nullable private final Integer stageAttemptId;
    @Nullable private final Long taskAttemptId;
    @Nullable private final Integer taskIndex;
    @Nullable private final Integer attemptNumber;
    @Nullable private final String executorId;
    @Nullable private final String host;
    @Nullable private final Long threadId;
    @Nullable private final String filePath;
    @Nullable private final String outputPath;
    @Nullable private final String objectRequestId;
    @Nullable private final String objectOperation;
    @Nullable private final Long durationMs;
    @Nullable private final Long rows;
    @Nullable private final Long bytes;
    @Nullable private final Integer queueDepth;
    @Nullable private final Integer runtimeThreads;
    @Nullable private final Long nativeMemoryBytes;
    @Nullable private final Long peakBufferedBytes;
    @Nullable private final String metricsJson;
    @Nullable private final String errorClass;
    @Nullable private final String errorMessage;
    @Nullable private final String stackTrace;

    @JsonCreator
    public NativeIOEvent(
            @JsonProperty("version") int version,
            @JsonProperty("event_id") String eventId,
            @JsonProperty("event_time") long eventTime,
            @JsonProperty("event_type") NativeIOEventType eventType,
            @JsonProperty("operation_id") String operationId,
            @JsonProperty("operation_name") String operationName,
            @JsonProperty("phase") @Nullable NativeIOPhase phase,
            @JsonProperty("sql_execution_id") @Nullable Long sqlExecutionId,
            @JsonProperty("stage_id") @Nullable Integer stageId,
            @JsonProperty("stage_attempt_id") @Nullable Integer stageAttemptId,
            @JsonProperty("task_attempt_id") @Nullable Long taskAttemptId,
            @JsonProperty("task_index") @Nullable Integer taskIndex,
            @JsonProperty("attempt_number") @Nullable Integer attemptNumber,
            @JsonProperty("executor_id") @Nullable String executorId,
            @JsonProperty("host") @Nullable String host,
            @JsonProperty("thread_id") @Nullable Long threadId,
            @JsonProperty("file_path") @Nullable String filePath,
            @JsonProperty("output_path") @Nullable String outputPath,
            @JsonProperty("object_request_id") @Nullable String objectRequestId,
            @JsonProperty("object_operation") @Nullable String objectOperation,
            @JsonProperty("duration_ms") @Nullable Long durationMs,
            @JsonProperty("rows") @Nullable Long rows,
            @JsonProperty("bytes") @Nullable Long bytes,
            @JsonProperty("queue_depth") @Nullable Integer queueDepth,
            @JsonProperty("runtime_threads") @Nullable Integer runtimeThreads,
            @JsonProperty("native_memory_bytes") @Nullable Long nativeMemoryBytes,
            @JsonProperty("peak_buffered_bytes") @Nullable Long peakBufferedBytes,
            @JsonProperty("metrics_json") @Nullable String metricsJson,
            @JsonProperty("error_class") @Nullable String errorClass,
            @JsonProperty("error_message") @Nullable String errorMessage,
            @JsonProperty("stack_trace") @Nullable String stackTrace) {
        this.version = version <= 0 ? VERSION : version;
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.operationId = operationId;
        this.operationName = operationName;
        this.phase = phase;
        this.sqlExecutionId = sqlExecutionId;
        this.stageId = stageId;
        this.stageAttemptId = stageAttemptId;
        this.taskAttemptId = taskAttemptId;
        this.taskIndex = taskIndex;
        this.attemptNumber = attemptNumber;
        this.executorId = executorId;
        this.host = host;
        this.threadId = threadId;
        this.filePath = filePath;
        this.outputPath = outputPath;
        this.objectRequestId = objectRequestId;
        this.objectOperation = objectOperation;
        this.durationMs = durationMs;
        this.rows = rows;
        this.bytes = bytes;
        this.queueDepth = queueDepth;
        this.runtimeThreads = runtimeThreads;
        this.nativeMemoryBytes = nativeMemoryBytes;
        this.peakBufferedBytes = peakBufferedBytes;
        this.metricsJson = metricsJson;
        this.errorClass = errorClass;
        this.errorMessage = errorMessage;
        this.stackTrace = stackTrace;
    }

    public static Builder builder(
            String eventId,
            long eventTime,
            NativeIOEventType eventType,
            String operationId,
            String operationName) {
        return new Builder(eventId, eventTime, eventType, operationId, operationName);
    }

    public static Builder builder(
            NativeIOEventType eventType, String operationId, String operationName) {
        return builder(UUID.randomUUID().toString(), System.currentTimeMillis(), eventType, operationId, operationName);
    }

    public Builder toBuilder() {
        return new Builder(eventId, eventTime, eventType, operationId, operationName)
                .withPhase(phase)
                .withSqlExecutionId(sqlExecutionId)
                .withStageId(stageId)
                .withStageAttemptId(stageAttemptId)
                .withTaskAttemptId(taskAttemptId)
                .withTaskIndex(taskIndex)
                .withAttemptNumber(attemptNumber)
                .withExecutorId(executorId)
                .withHost(host)
                .withThreadId(threadId)
                .withFilePath(filePath)
                .withOutputPath(outputPath)
                .withObjectRequestId(objectRequestId)
                .withObjectOperation(objectOperation)
                .withDurationMs(durationMs)
                .withRows(rows)
                .withBytes(bytes)
                .withQueueDepth(queueDepth)
                .withRuntimeThreads(runtimeThreads)
                .withNativeMemoryBytes(nativeMemoryBytes)
                .withPeakBufferedBytes(peakBufferedBytes)
                .withMetricsJson(metricsJson)
                .withErrorClass(errorClass)
                .withErrorMessage(errorMessage)
                .withStackTrace(stackTrace);
    }

    @JsonProperty("version")
    public int version() {
        return version;
    }

    @JsonProperty("event_id")
    public String eventId() {
        return eventId;
    }

    @JsonProperty("event_time")
    public long eventTime() {
        return eventTime;
    }

    @JsonProperty("event_type")
    public NativeIOEventType eventType() {
        return eventType;
    }

    @JsonProperty("operation_id")
    public String operationId() {
        return operationId;
    }

    @JsonProperty("operation_name")
    public String operationName() {
        return operationName;
    }

    @JsonProperty("phase")
    @Nullable
    public NativeIOPhase phase() {
        return phase;
    }

    @JsonProperty("sql_execution_id")
    @Nullable
    public Long sqlExecutionId() {
        return sqlExecutionId;
    }

    @JsonProperty("stage_id")
    @Nullable
    public Integer stageId() {
        return stageId;
    }

    @JsonProperty("stage_attempt_id")
    @Nullable
    public Integer stageAttemptId() {
        return stageAttemptId;
    }

    @JsonProperty("task_attempt_id")
    @Nullable
    public Long taskAttemptId() {
        return taskAttemptId;
    }

    @JsonProperty("task_index")
    @Nullable
    public Integer taskIndex() {
        return taskIndex;
    }

    @JsonProperty("attempt_number")
    @Nullable
    public Integer attemptNumber() {
        return attemptNumber;
    }

    @JsonProperty("executor_id")
    @Nullable
    public String executorId() {
        return executorId;
    }

    @JsonProperty("host")
    @Nullable
    public String host() {
        return host;
    }

    @JsonProperty("thread_id")
    @Nullable
    public Long threadId() {
        return threadId;
    }

    @JsonProperty("file_path")
    @Nullable
    public String filePath() {
        return filePath;
    }

    @JsonProperty("output_path")
    @Nullable
    public String outputPath() {
        return outputPath;
    }

    @JsonProperty("object_request_id")
    @Nullable
    public String objectRequestId() {
        return objectRequestId;
    }

    @JsonProperty("object_operation")
    @Nullable
    public String objectOperation() {
        return objectOperation;
    }

    @JsonProperty("duration_ms")
    @Nullable
    public Long durationMs() {
        return durationMs;
    }

    @JsonProperty("rows")
    @Nullable
    public Long rows() {
        return rows;
    }

    @JsonProperty("bytes")
    @Nullable
    public Long bytes() {
        return bytes;
    }

    @JsonProperty("queue_depth")
    @Nullable
    public Integer queueDepth() {
        return queueDepth;
    }

    @JsonProperty("runtime_threads")
    @Nullable
    public Integer runtimeThreads() {
        return runtimeThreads;
    }

    @JsonProperty("native_memory_bytes")
    @Nullable
    public Long nativeMemoryBytes() {
        return nativeMemoryBytes;
    }

    @JsonProperty("peak_buffered_bytes")
    @Nullable
    public Long peakBufferedBytes() {
        return peakBufferedBytes;
    }

    @JsonProperty("metrics_json")
    @Nullable
    public String metricsJson() {
        return metricsJson;
    }

    @JsonProperty("error_class")
    @Nullable
    public String errorClass() {
        return errorClass;
    }

    @JsonProperty("error_message")
    @Nullable
    public String errorMessage() {
        return errorMessage;
    }

    @JsonProperty("stack_trace")
    @Nullable
    public String stackTrace() {
        return stackTrace;
    }

    /** Builder for {@link NativeIOEvent}. */
    public static final class Builder {
        private final String eventId;
        private final long eventTime;
        private final NativeIOEventType eventType;
        private final String operationId;
        private final String operationName;
        @Nullable private NativeIOPhase phase;
        @Nullable private Long sqlExecutionId;
        @Nullable private Integer stageId;
        @Nullable private Integer stageAttemptId;
        @Nullable private Long taskAttemptId;
        @Nullable private Integer taskIndex;
        @Nullable private Integer attemptNumber;
        @Nullable private String executorId;
        @Nullable private String host;
        @Nullable private Long threadId;
        @Nullable private String filePath;
        @Nullable private String outputPath;
        @Nullable private String objectRequestId;
        @Nullable private String objectOperation;
        @Nullable private Long durationMs;
        @Nullable private Long rows;
        @Nullable private Long bytes;
        @Nullable private Integer queueDepth;
        @Nullable private Integer runtimeThreads;
        @Nullable private Long nativeMemoryBytes;
        @Nullable private Long peakBufferedBytes;
        @Nullable private String metricsJson;
        @Nullable private String errorClass;
        @Nullable private String errorMessage;
        @Nullable private String stackTrace;

        private Builder(
                String eventId,
                long eventTime,
                NativeIOEventType eventType,
                String operationId,
                String operationName) {
            this.eventId = eventId;
            this.eventTime = eventTime;
            this.eventType = eventType;
            this.operationId = operationId;
            this.operationName = operationName;
        }

        public Builder withPhase(@Nullable NativeIOPhase phase) {
            this.phase = phase;
            return this;
        }

        public Builder withSqlExecutionId(@Nullable Long sqlExecutionId) {
            this.sqlExecutionId = sqlExecutionId;
            return this;
        }

        public Builder withStageId(@Nullable Integer stageId) {
            this.stageId = stageId;
            return this;
        }

        public Builder withStageAttemptId(@Nullable Integer stageAttemptId) {
            this.stageAttemptId = stageAttemptId;
            return this;
        }

        public Builder withTaskAttemptId(@Nullable Long taskAttemptId) {
            this.taskAttemptId = taskAttemptId;
            return this;
        }

        public Builder withTaskIndex(@Nullable Integer taskIndex) {
            this.taskIndex = taskIndex;
            return this;
        }

        public Builder withAttemptNumber(@Nullable Integer attemptNumber) {
            this.attemptNumber = attemptNumber;
            return this;
        }

        public Builder withExecutorId(@Nullable String executorId) {
            this.executorId = executorId;
            return this;
        }

        public Builder withHost(@Nullable String host) {
            this.host = host;
            return this;
        }

        public Builder withThreadId(@Nullable Long threadId) {
            this.threadId = threadId;
            return this;
        }

        public Builder withFilePath(@Nullable String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder withOutputPath(@Nullable String outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        public Builder withObjectRequestId(@Nullable String objectRequestId) {
            this.objectRequestId = objectRequestId;
            return this;
        }

        public Builder withObjectOperation(@Nullable String objectOperation) {
            this.objectOperation = objectOperation;
            return this;
        }

        public Builder withDurationMs(@Nullable Long durationMs) {
            this.durationMs = durationMs;
            return this;
        }

        public Builder withRows(@Nullable Long rows) {
            this.rows = rows;
            return this;
        }

        public Builder withBytes(@Nullable Long bytes) {
            this.bytes = bytes;
            return this;
        }

        public Builder withQueueDepth(@Nullable Integer queueDepth) {
            this.queueDepth = queueDepth;
            return this;
        }

        public Builder withRuntimeThreads(@Nullable Integer runtimeThreads) {
            this.runtimeThreads = runtimeThreads;
            return this;
        }

        public Builder withNativeMemoryBytes(@Nullable Long nativeMemoryBytes) {
            this.nativeMemoryBytes = nativeMemoryBytes;
            return this;
        }

        public Builder withPeakBufferedBytes(@Nullable Long peakBufferedBytes) {
            this.peakBufferedBytes = peakBufferedBytes;
            return this;
        }

        public Builder withMetricsJson(@Nullable String metricsJson) {
            this.metricsJson = metricsJson;
            return this;
        }

        public Builder withErrorClass(@Nullable String errorClass) {
            this.errorClass = errorClass;
            return this;
        }

        public Builder withErrorMessage(@Nullable String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder withStackTrace(@Nullable String stackTrace) {
            this.stackTrace = stackTrace;
            return this;
        }

        public NativeIOEvent build() {
            return new NativeIOEvent(
                    VERSION,
                    eventId,
                    eventTime,
                    eventType,
                    operationId,
                    operationName,
                    phase,
                    sqlExecutionId,
                    stageId,
                    stageAttemptId,
                    taskAttemptId,
                    taskIndex,
                    attemptNumber,
                    executorId,
                    host,
                    threadId,
                    filePath,
                    outputPath,
                    objectRequestId,
                    objectOperation,
                    durationMs,
                    rows,
                    bytes,
                    queueDepth,
                    runtimeThreads,
                    nativeMemoryBytes,
                    peakBufferedBytes,
                    metricsJson,
                    errorClass,
                    errorMessage,
                    stackTrace);
        }
    }
}
