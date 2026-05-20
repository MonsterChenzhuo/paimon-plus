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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Converts Paimon predicates to the first native export JSON predicate protocol. */
public final class NativeExportPredicateJson {

    public static final String FORMAT = "paimon-json-v1";
    public static final String TRUE_JSON = "{\"op\":\"true\"}";

    private NativeExportPredicateJson() {}

    public static NativeApplicability validate(@Nullable Predicate predicate) {
        try {
            toJson(predicate);
            return NativeApplicability.yes();
        } catch (UnsupportedOperationException e) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_UNSUPPORTED_PREDICATE, e.getMessage());
        }
    }

    public static String toJson(@Nullable Predicate predicate) {
        if (predicate == null) {
            return TRUE_JSON;
        }
        return JsonSerdeUtil.toFlatJson(toNode(predicate));
    }

    private static Map<String, Object> toNode(Predicate predicate) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            String op;
            if (compound.function() instanceof And) {
                op = "and";
            } else if (compound.function() instanceof Or) {
                op = "or";
            } else {
                throw new UnsupportedOperationException(
                        "unsupported compound predicate: " + compound.function());
            }
            List<Map<String, Object>> children = new ArrayList<>();
            for (Predicate child : compound.children()) {
                children.add(toNode(child));
            }
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("op", op);
            node.put("children", children);
            return node;
        }

        if (!(predicate instanceof LeafPredicate)) {
            throw new UnsupportedOperationException(
                    "unsupported predicate class: " + predicate.getClass().getName());
        }

        LeafPredicate leaf = (LeafPredicate) predicate;
        Optional<FieldRef> fieldRef = leaf.fieldRefOptional();
        if (!fieldRef.isPresent()) {
            throw new UnsupportedOperationException(
                    "only direct field predicates are supported by native export");
        }
        FieldRef field = fieldRef.get();
        ensureSupportedType(field.type());
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("field", field.name());
        if (leaf.function() instanceof Equal) {
            node.put("op", "eq");
            node.put("literal", literal(field.type(), leaf.literals().get(0)));
        } else if (leaf.function() instanceof NotEqual) {
            node.put("op", "ne");
            node.put("literal", literal(field.type(), leaf.literals().get(0)));
        } else if (leaf.function() instanceof LessThan) {
            node.put("op", "lt");
            node.put("literal", literal(field.type(), leaf.literals().get(0)));
        } else if (leaf.function() instanceof LessOrEqual) {
            node.put("op", "le");
            node.put("literal", literal(field.type(), leaf.literals().get(0)));
        } else if (leaf.function() instanceof GreaterThan) {
            node.put("op", "gt");
            node.put("literal", literal(field.type(), leaf.literals().get(0)));
        } else if (leaf.function() instanceof GreaterOrEqual) {
            node.put("op", "ge");
            node.put("literal", literal(field.type(), leaf.literals().get(0)));
        } else if (leaf.function() instanceof IsNull) {
            node.put("op", "is_null");
        } else if (leaf.function() instanceof IsNotNull) {
            node.put("op", "is_not_null");
        } else if (leaf.function() instanceof In) {
            node.put("op", "in");
            List<Map<String, Object>> literals = new ArrayList<>();
            for (Object value : leaf.literals()) {
                literals.add(literal(field.type(), value));
            }
            node.put("literals", literals);
        } else {
            throw new UnsupportedOperationException(
                    "unsupported leaf predicate: " + leaf.function());
        }
        return node;
    }

    private static void ensureSupportedType(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
                return;
            default:
                throw new UnsupportedOperationException(
                        "unsupported predicate literal type: " + root);
        }
    }

    private static Map<String, Object> literal(DataType type, Object value) {
        Map<String, Object> literal = new LinkedHashMap<>();
        DataTypeRoot root = type.getTypeRoot();
        literal.put("type", root.name());
        if (value == null) {
            literal.put("value", null);
            return literal;
        }
        if (value instanceof BinaryString) {
            literal.put("value", value.toString());
        } else {
            literal.put("value", value);
        }
        return literal;
    }
}
