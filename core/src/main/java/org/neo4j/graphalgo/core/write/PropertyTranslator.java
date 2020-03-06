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
package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public interface PropertyTranslator<T> {

    Value toProperty(int propertyId, T data, long nodeId);

    double toDouble(final T data, final long nodeId);

    interface OfDouble<T> extends PropertyTranslator<T> {
        double toDouble(final T data, final long nodeId);

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            return Values.doubleValue(toDouble(data, nodeId));
        }
    }

    interface OfInt<T> extends PropertyTranslator<T> {
        int toInt(final T data, final long nodeId);

        @Override
        default double toDouble(final T data, final long nodeId) {
            return toInt(data, nodeId);
        }

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final int value = toInt(data, nodeId);
            return Values.intValue(value);
        }
    }

    interface OfLong<T> extends PropertyTranslator<T> {
        long toLong(final T data, final long nodeId);

        @Override
        default double toDouble(final T data, final long nodeId) {
            return toLong(data, nodeId);
        }

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final long value = toLong(data, nodeId);
            return Values.longValue(value);
        }
    }

    interface OfLongArray<T> extends PropertyTranslator<T> {
        long[] toLongArray(final T data, final long nodeId);

        @Override
        default double toDouble(final T data, final long nodeId) {
            throw new UnsupportedOperationException("Can not translate list property to single double value.");
        }

        @Override
        default Value toProperty(
            int propertyId,
            T data,
            long nodeId) {
            final long[] value = toLongArray(data, nodeId);
            return Values.longArray(value);
        }
    }

    @FunctionalInterface
    interface SeededDataAccessFunction<T> {

        long getValue(T data, long nodeId);

    }

    final class OfLongIfChanged<T> implements PropertyTranslator<T> {

        private final NodeProperties currentProperties;
        private final SeededDataAccessFunction<T> newPropertiesFn;

        public OfLongIfChanged(NodeProperties currentProperties, SeededDataAccessFunction<T> newPropertiesFn) {
            this.currentProperties = currentProperties;
            this.newPropertiesFn = newPropertiesFn;
        }

        @Override
        public double toDouble(final T data, final long nodeId) {
            return newPropertiesFn.getValue(data, nodeId);
        }

        @Override
        public Value toProperty(int propertyId, T data, long nodeId) {
            double seedValue = currentProperties.nodeProperty(nodeId, Double.NaN);
            long computedValue = newPropertiesFn.getValue(data, nodeId);
            return Double.isNaN(seedValue) || ((long) seedValue != computedValue) ? Values.longValue(computedValue) : null;
        }
    }
}
