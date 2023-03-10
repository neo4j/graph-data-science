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
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ValueConverter {

    public static ValueType valueType(Value value) {
        if (value instanceof IntegralValue) {
            return ValueType.LONG;
        } else if (value instanceof FloatingPointValue) {
            return ValueType.DOUBLE;
        } else if (value instanceof LongArray) {
            return ValueType.LONG_ARRAY;
        } else if (value instanceof DoubleArray) {
            return ValueType.DOUBLE_ARRAY;
        } else if (value instanceof FloatArray) {
            return ValueType.FLOAT_ARRAY;
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Loading of values of type %s is currently not supported",
                value.getTypeName()
            ));
        }
    }

    public static Value toValue(@NotNull AnyValue value) {
        if (value == NoValue.NO_VALUE) {
            return NoValue.NO_VALUE;
        } else if (value.isSequenceValue()) {
            return castToNumericArrayOrFail(value);
        } else if (value instanceof Value) {
            var storableValue = (Value) value;
            if (storableValue.valueGroup() != ValueGroup.NUMBER) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Unsupported GDS node property of type `%s`.",
                    storableValue.getTypeName()
                ));
            }
            return storableValue;
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Unsupported GDS node property of type `%s`.",
                value.getTypeName()
            ));
        }
    }
    private static ArrayValue castToNumericArrayOrFail(AnyValue value) {
        if (value instanceof ListValue) {
            return castToNumericArrayOrFail((ListValue) value);
        } else if (value instanceof ArrayValue){
            return assertNumberArray((ArrayValue) value);
        } else {
            throw failOnBadList(value);
        }
    }

    private static ArrayValue assertNumberArray(ArrayValue array) {
        if (array.valueGroup() != ValueGroup.NUMBER_ARRAY) {
            throw failOnBadList(array);
        }
        return array;
    }
    @NotNull
    private static ArrayValue castToNumericArrayOrFail(ListValue listValue) {
        if (listValue.isEmpty()) {
            // encode as long array
            return Values.EMPTY_LONG_ARRAY;
        }

        var firstValue = listValue.head();
        try {
            if (firstValue instanceof LongValue) {
                var longArray = new long[listValue.size()];
                var iterator = listValue.iterator();
                for (int i = 0; i < listValue.size() && iterator.hasNext(); i++) {
                    longArray[i] = ((LongValue) iterator.next()).longValue();
                }
                return Values.longArray(longArray);
            } else if (firstValue instanceof DoubleValue) {
                var doubleArray = new double[listValue.size()];
                var iterator = listValue.iterator();
                for (int i = 0; i < listValue.size() && iterator.hasNext(); i++) {
                    doubleArray[i] = ((DoubleValue) iterator.next()).doubleValue();
                }
                return Values.doubleArray(doubleArray);
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

    private ValueConverter() {}
}
