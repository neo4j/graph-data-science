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
package org.neo4j.gds.core.compression.packed;

import org.eclipse.collections.api.block.function.primitive.ObjectIntToObjectFunction;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.PropertyCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

public class PackedPropertyList implements AdjacencyProperties {

    private final HugeObjectArray<Compressed> adjacencies;
    private final int propertyIndex;

    PackedPropertyList(HugeObjectArray<Compressed> adjacencies, int propertyIndex) {
        this.adjacencies = adjacencies;
        this.propertyIndex = propertyIndex;
    }

    @Override
    public PropertyCursor propertyCursor(long node, double fallbackValue) {
        return this.createCursor(node, fallbackValue, PackedPropertyCursor::new);
    }

    @Override
    public PropertyCursor propertyCursor(PropertyCursor reuse, long node, double fallbackValue) {
        if (reuse instanceof PackedPropertyCursor) {
            PackedPropertyCursor cursor = (PackedPropertyCursor) reuse;
            return this.createCursor(node, fallbackValue, cursor::init);
        } else {
            return this.propertyCursor(node, fallbackValue);
        }
    }

    @Override
    public PropertyCursor rawPropertyCursor() {
        return new PackedPropertyCursor();
    }

    private PropertyCursor createCursor(long node, double fallbackValue, ObjectIntToObjectFunction<long[], PropertyCursor> create) {
        var compressed = this.adjacencies.getOrDefault(node, Compressed.EMPTY);
        var degree = compressed.length();

        if (degree == 0) {
            return PropertyCursor.empty();
        }

        long[][] properties = compressed.properties();
        if (properties == null) {
            return new FallbackPropertyCursor(fallbackValue, degree);
        }

        return create.valueOf(properties[this.propertyIndex], degree);
    }
}

final class PackedPropertyCursor implements PropertyCursor {

    private long[] currentProperties;
    private int maxTargets;
    private int currentPosition;

    PackedPropertyCursor() {}

    PackedPropertyCursor(long[] properties, int degree) {
        this.init(properties, degree);
    }

    PackedPropertyCursor init(long[] properties, int degree) {
        this.currentProperties = properties;
        this.maxTargets = degree;
        this.currentPosition = 0;
        return this;
    }

    @Override
    public void init(long node, int degree) {
        throw new UnsupportedOperationException("FallbackPropertyCursor must be re-used via its PackedAdjacencyList");
    }

    @Override
    public boolean hasNextLong() {
        return this.currentPosition < this.maxTargets;
    }

    @Override
    public long nextLong() {
        return this.currentProperties[this.currentPosition++];
    }

    @Override
    public void close() {
        this.currentProperties = null;
        this.maxTargets = 0;
        this.currentPosition = 0;
    }
}


final class FallbackPropertyCursor implements PropertyCursor {

    private final long fallbackValue;
    private final int maxTargets;
    private int currentPosition;

    FallbackPropertyCursor(double fallbackValue, int maxTargets) {
        this.fallbackValue = Double.doubleToLongBits(fallbackValue);
        this.maxTargets = maxTargets;
    }

    @Override
    public void init(long node, int degree) {
        throw new UnsupportedOperationException("FallbackPropertyCursor cannot be re-used");
    }

    @Override
    public boolean hasNextLong() {
        return this.currentPosition < this.maxTargets;
    }

    @Override
    public long nextLong() {
        return this.fallbackValue;
    }

    @Override
    public void close() {
        this.currentPosition = this.maxTargets;
    }
}

