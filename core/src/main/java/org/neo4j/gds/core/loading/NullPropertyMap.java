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

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * {@link org.neo4j.gds.api.properties.nodes.NodePropertyValues} implementation which always returns
 * a given default property value upon invocation.
 */
public abstract class NullPropertyMap implements NodePropertyValues {

    @Override
    public Optional<Integer> dimension() {
        return Optional.of(1);
    }

    public static class DoubleNullPropertyMap extends NullPropertyMap {
        private final double defaultValue;

        public DoubleNullPropertyMap(double defaultValue) {this.defaultValue = defaultValue;}

        @Override
        public double doubleValue(long nodeId) {
            return this.defaultValue;
        }

        @Override
        public Object getObject(long nodeId) {
            return doubleValue(nodeId);
        }

        @Override
        public Value value(long nodeId) {
            return Values.doubleValue(defaultValue);
        }

        @Override
        public ValueType valueType() {
            return ValueType.DOUBLE;
        }

        @Override
        public OptionalDouble getMaxDoublePropertyValue() {
            return OptionalDouble.empty();
        }

        @Override
        public long valuesStored() {
            return 0;
        }
    }

    public static class LongNullPropertyMap extends NullPropertyMap implements LongNodePropertyValues {
        private final long defaultValue;

        public LongNullPropertyMap(long defaultValue) {this.defaultValue = defaultValue;}

        @Override
        public long longValue(long nodeId) {
            return this.defaultValue;
        }

        @Override
        public Object getObject(long nodeId) {
            return longValue(nodeId);
        }

        @Override
        public Value value(long nodeId) {
            return Values.longValue(defaultValue);
        }

        @Override
        public OptionalLong getMaxLongPropertyValue() {
            return OptionalLong.empty();
        }

        @Override
        public ValueType valueType() {
            return ValueType.LONG;
        }

        @Override
        public long valuesStored() {
            return 0;
        }
    }

}
