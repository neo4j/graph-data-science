/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.core.loading;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.values.Array;
import org.neo4j.gds.values.GdsNoValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.primitive.PrimitiveValues;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.virtual.ListValue;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GdsNeo4jValueConverter {

    public static GdsValue toValue(@NotNull AnyValue value) {
        if (value == NoValue.NO_VALUE) {
            return GdsNoValue.NO_VALUE;
        }
        if (value.isSequenceValue()) { // ArrayValue or ListValue
            return convertSequenceValueOrFail((SequenceValue) value);
        }
        if (value instanceof Value storableValue && storableValue.valueGroup() == ValueGroup.NUMBER) {
            if (storableValue instanceof org.neo4j.values.storable.FloatValue floatValue) {
                return PrimitiveValues.floatingPointValue(floatValue.floatValue());
            } else if (storableValue instanceof org.neo4j.values.storable.DoubleValue doubleValue) {
                return PrimitiveValues.floatingPointValue(doubleValue.doubleValue());
            } else if (storableValue instanceof IntegralValue integralValue) {
                return PrimitiveValues.longValue(integralValue.longValue());
            }
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Unsupported conversion to GDS Value from Neo4j Value with type `%s`.",
            value.getTypeName()
        ));
    }

    private static GdsValue convertSequenceValueOrFail(SequenceValue value) {
        if (value instanceof ListValue listValue) {
            return convertListValueOrFail(listValue);
        } else if (value instanceof ArrayValue arrayValue) {
            return convertArrayValueOrFail(arrayValue);
        } else {
            throw failOnBadInput(value);
        }
    }

    @NotNull
    private static Array convertListValueOrFail(ListValue listValue) {
        if (listValue.isEmpty()) {
            // encode as long array
            return PrimitiveValues.EMPTY_LONG_ARRAY;
        }
        try {
            return convertArrayValueOrFail(listValue.toStorableArray());
        } catch (RuntimeException e) {
            throw failOnBadInput(listValue);
        }
    }

    @NotNull
    private static Array convertArrayValueOrFail(ArrayValue array) {
        if (array.valueGroup() != ValueGroup.NUMBER_ARRAY) {
            throw failOnBadInput(array);
        }
        if (array.isEmpty()) {
            return PrimitiveValues.EMPTY_LONG_ARRAY;
        }
        var arrayCopy = array.asObjectCopy();
        if (arrayCopy instanceof long[]) {
            return PrimitiveValues.longArray((long[]) arrayCopy);
        } else if (arrayCopy instanceof double[]) {
            return PrimitiveValues.doubleArray((double[]) arrayCopy);
        } else {
            throw failOnBadInput(array);
        }
    }

    private static IllegalArgumentException failOnBadInput(SequenceValue badInput) {
        return new IllegalArgumentException(
            formatWithLocale(
                "Unsupported conversion to GDS Value from Neo4j Value `%s`.",
                badInput
            )
        );
    }

    private GdsNeo4jValueConverter() {}
}
