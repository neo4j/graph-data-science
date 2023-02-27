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
package org.neo4j.gds.api.properties.nodes;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.PropertyValues;
import org.neo4j.values.storable.Value;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface NodePropertyValues extends PropertyValues {

    default double doubleValue(long nodeId) {
        throw unsupportedTypeException(ValueType.DOUBLE);
    }

    default long longValue(long nodeId) {
        throw unsupportedTypeException(ValueType.LONG);
    }

    @Nullable
    default double[] doubleArrayValue(long nodeId) {
        throw unsupportedTypeException(ValueType.DOUBLE_ARRAY);
    }

    @Nullable
    default float[] floatArrayValue(long nodeId) {
        throw unsupportedTypeException(ValueType.FLOAT_ARRAY);
    }

    @Nullable
    default long[] longArrayValue(long nodeId) {
        throw unsupportedTypeException(ValueType.LONG_ARRAY);
    }

    @Nullable
    Object getObject(long nodeId);

    Value value(long nodeId);

    /**
     * The dimension of the properties.
     * For scalar values, this is 1.
     * For arrays, this is the length of the array stored for the 0th node id.
     * If that array is {@code null}, this method returns {@link Optional#empty()}.
     *
     * @return the dimension of the properties stored, or empty if the dimension cannot easily be retrieved.
     */
    Optional<Integer> dimension();

    /**
     * @return the maximum long value contained in the mapping or an empty {@link OptionalLong} if the mapping is
     *         empty or the feature is not supported.
     * @throws java.lang.UnsupportedOperationException if the type is not coercible into a long.
     */
    default OptionalLong getMaxLongPropertyValue() {
        if (valueType() == ValueType.LONG) {
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
        if (valueType() == ValueType.DOUBLE) {
            throw new UnsupportedOperationException(formatWithLocale("%s does not overwrite `getMaxDoublePropertyValue`", getClass().getSimpleName()));
        } else {
            throw unsupportedTypeException(ValueType.DOUBLE);
        }
    }
}
