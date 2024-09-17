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
import org.neo4j.gds.values.GdsNoValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.primitive.PrimitiveValues;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
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
            return castToNumericArrayOrFail(value);
        }
        if (value instanceof Value storableValue) {
            if (storableValue.valueGroup() != ValueGroup.NUMBER) {
                throw new IllegalArgumentException(formatWithLocale("Unsupported GDS node property of type `%s`.", storableValue.getTypeName()));
            } else if (storableValue instanceof org.neo4j.values.storable.FloatingPointValue floatingPointValue) {
                return PrimitiveValues.floatingPointValue(floatingPointValue.floatValue());
            } else if (storableValue instanceof IntegralValue integralValue) {
                return PrimitiveValues.longValue(integralValue.longValue());
            }
        }
        throw new IllegalArgumentException(formatWithLocale("Unsupported GDS node property of type `%s`.", value.getTypeName()));
    }

    private static GdsValue castToNumericArrayOrFail(AnyValue value) {
        if (value instanceof ListValue listValue) {
            return castToNumericArrayOrFail(listValue);
        } else if (value instanceof ArrayValue arrayValue){
        return assertNumberArray(arrayValue);
        } else {
            throw failOnBadList(value);
        }
    }

    private static org.neo4j.gds.values.Array assertNumberArray(ArrayValue array) {
        if (array.valueGroup() != ValueGroup.NUMBER_ARRAY) {
            throw failOnBadList(array);
        }
        if (array.isEmpty()) {
            return PrimitiveValues.EMPTY_LONG_ARRAY;
        }

        var firstValue = array.value(0);
        try {
            if (firstValue instanceof LongValue) {
                var longArray = new long[array.length()];
                var iterator = array.iterator();
                for (int i = 0; i < array.length() && iterator.hasNext(); i++) {
                    longArray[i] = ((LongValue) iterator.next()).longValue();
                }
                return PrimitiveValues.longArray(longArray);
            } else if (firstValue instanceof DoubleValue) {
                var doubleArray = new double[array.length()];
                var iterator = array.iterator();
                for (int i = 0; i < array.length() && iterator.hasNext(); i++) {
                    doubleArray[i] = ((DoubleValue) iterator.next()).doubleValue();
                }
                return PrimitiveValues.doubleArray(doubleArray);
            } else {
                throw failOnBadList(array);
            }
        } catch (ClassCastException c) {
            throw failOnBadList(array);
        }
    }

    @NotNull
    private static org.neo4j.gds.values.Array castToNumericArrayOrFail(ListValue listValue) {
        if (listValue.isEmpty()) {
            // encode as long array
            return PrimitiveValues.EMPTY_LONG_ARRAY;
        }

        var firstValue = listValue.head();
        try {
            if (firstValue instanceof LongValue) {
                var longArray = new long[listValue.size()];
                var iterator = listValue.iterator();
                for (int i = 0; i < listValue.size() && iterator.hasNext(); i++) {
                    longArray[i] = ((LongValue) iterator.next()).longValue();
                }
                return PrimitiveValues.longArray(longArray);
            } else if (firstValue instanceof DoubleValue) {
                var doubleArray = new double[listValue.size()];
                var iterator = listValue.iterator();
                for (int i = 0; i < listValue.size() && iterator.hasNext(); i++) {
                    doubleArray[i] = ((DoubleValue) iterator.next()).doubleValue();
                }
                return PrimitiveValues.doubleArray(doubleArray);
            } else {
                throw failOnBadList(listValue);
            }
        } catch (ClassCastException c) {
            throw failOnBadList(listValue);
        }
    }

    private static IllegalArgumentException failOnBadList(AnyValue badList) {
        return new IllegalArgumentException(formatWithLocale(
            "Only lists of uniformly typed numbers are supported as GDS node properties, but found an unsupported list `%s`.",
            badList
        ));
    }

    private GdsNeo4jValueConverter() {}
}
