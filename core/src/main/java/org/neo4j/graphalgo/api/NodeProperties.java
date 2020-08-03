/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;

import java.util.OptionalDouble;
import java.util.OptionalLong;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface NodeProperties {

    default double getDouble(long nodeId) {
        throw unsupportedTypeException(ValueType.DOUBLE);
    }

    default long getLong(long nodeId) {
        throw unsupportedTypeException(ValueType.LONG);
    };

    default double[] getDoubleArray(long nodeId) {
        throw unsupportedTypeException(ValueType.DOUBLE_ARRAY);
    }

    default long[] getLongArray(long nodeId) {
        throw unsupportedTypeException(ValueType.LONG_ARRAY);
    }

    Object getObject(long nodeId);

    ValueType getType();

    Value getValue(long nodeId);

    default double getDouble(long nodeId, double defaultValue) {
        return getDouble(nodeId);
    }

    default long getLong(long nodeId, long defaultValue) {
        return getLong(nodeId);
    }

    default double[] getDoubleArray(long nodeId, double[] defaultValue) {
        return getDoubleArray(nodeId);
    }

    default long[] getLongArray(long nodeId, long[] defaultValue) {
        return getLongArray(nodeId);
    }

    default Object getObject(long nodeId, Object defaultValue) {
        return getObject(nodeId);
    }

    /**
     * Release internal data structures and return an estimate how many bytes were freed.
     *
     * Note that the mapping is not usable afterwards.
     */
    default long release() {
        return 0;
    }

    /**
     * @return the number of values stored.
     */
    default long size() {
        return 0;
    }

    /**
     * @return the maximum long value contained in the mapping or an empty {@link OptionalLong} if the mapping is
     *         empty or the feature is not supported.
     * @throws java.lang.UnsupportedOperationException if the type is not coercible into a long.
     */
    default OptionalLong getMaxLongPropertyValue() {
        if (getType() == ValueType.DOUBLE) {
            var maxDoublePropertyValue = getMaxDoublePropertyValue();

            if (maxDoublePropertyValue.isPresent()) {
                return OptionalLong.of(((Double)maxDoublePropertyValue.getAsDouble()).longValue());
            } else {
                return OptionalLong.empty();
            }
        } else if (getType() == ValueType.LONG) {
            throw new UnsupportedOperationException(formatWithLocale("%s does not overwrite `getMaxLongPropertyValue`", getClass().getSimpleName()));
        } else {
            throw unsupportedTypeException(ValueType.LONG);

        }
    }

    /**
     * @return the maximum double value contained in the mapping or an empty {@link OptionalDouble} if the mapping is
     *         empty or the feature is not supported.
     * @throws java.lang.UnsupportedOperationException if the type is not coercible into a double.
     */
    default OptionalDouble getMaxDoublePropertyValue() {
        if (getType() == ValueType.LONG) {
            var maxLong = getMaxLongPropertyValue();

            if (maxLong.isPresent()) {
                return OptionalDouble.of(((Long)maxLong.getAsLong()).doubleValue());
            } else {
                return OptionalDouble.empty();
            }
        } else if (getType() == ValueType.DOUBLE) {
            throw new UnsupportedOperationException(formatWithLocale("%s does not overwrite `getMaxDoublePropertyValue`", getClass().getSimpleName()));
        } else {
            throw unsupportedTypeException(ValueType.DOUBLE);
        }
    }

    private UnsupportedOperationException unsupportedTypeException(ValueType expectedType) {
        return new UnsupportedOperationException(formatWithLocale("Tried to retrieve a value of type %s value from properties of type %s", expectedType, getType()));
    }
}
