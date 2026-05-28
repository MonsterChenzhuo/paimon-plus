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

package org.apache.paimon.nativeio.spark;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/** Spark {@link ColumnVector} wrapper over an Arrow {@link FieldVector}. */
class ArrowSparkColumnVector extends ColumnVector {

    private final FieldVector vector;
    private final Runnable closeAction;

    ArrowSparkColumnVector(DataType type, FieldVector vector, Runnable closeAction) {
        super(type);
        this.vector = vector;
        this.closeAction = closeAction;
    }

    @Override
    public void close() {
        closeAction.run();
    }

    @Override
    public boolean hasNull() {
        return vector.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
        return vector.getNullCount();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return vector.isNull(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return ((BitVector) vector).get(rowId) != 0;
    }

    @Override
    public byte getByte(int rowId) {
        return ((TinyIntVector) vector).get(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return ((SmallIntVector) vector).get(rowId);
    }

    @Override
    public int getInt(int rowId) {
        if (vector instanceof DateDayVector) {
            return ((DateDayVector) vector).get(rowId);
        }
        if (vector instanceof TimeMilliVector) {
            return ((TimeMilliVector) vector).get(rowId);
        }
        return ((IntVector) vector).get(rowId);
    }

    @Override
    public long getLong(int rowId) {
        if (vector instanceof TimeStampVector) {
            long value = ((TimeStampVector) vector).get(rowId);
            ArrowType.Timestamp type = (ArrowType.Timestamp) vector.getField().getType();
            TimeUnit unit = type.getUnit();
            if (unit == TimeUnit.SECOND) {
                return value * 1_000_000L;
            } else if (unit == TimeUnit.MILLISECOND) {
                return value * 1_000L;
            } else if (unit == TimeUnit.NANOSECOND) {
                return value / 1_000L;
            }
            return value;
        }
        return ((BigIntVector) vector).get(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return ((Float4Vector) vector).get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return ((Float8Vector) vector).get(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        throw new UnsupportedOperationException(
                "Array type is not supported by native columnar IO.");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
        throw new UnsupportedOperationException("Map type is not supported by native columnar IO.");
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        return Decimal.apply(((DecimalVector) vector).getObject(rowId), precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return UTF8String.fromBytes(((VarCharVector) vector).get(rowId));
    }

    @Override
    public byte[] getBinary(int rowId) {
        return ((VarBinaryVector) vector).get(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        throw new UnsupportedOperationException(
                "Nested type is not supported by native columnar IO.");
    }
}
