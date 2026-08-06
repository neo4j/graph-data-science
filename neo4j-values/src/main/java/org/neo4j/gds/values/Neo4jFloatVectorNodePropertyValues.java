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
package org.neo4j.gds.values;

import org.neo4j.gds.api.properties.nodes.FloatVectorNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Materializes a float array property as a Neo4j {@code Float32Vector} instead of a plain array
 */
public class Neo4jFloatVectorNodePropertyValues implements FloatVectorNodePropertyValues, Neo4jNodePropertyValues {

    private final NodePropertyValues internal;
    private final int dimension;

    public Neo4jFloatVectorNodePropertyValues(NodePropertyValues internal, int dimension) {
        this.internal = internal;
        this.dimension = dimension;
    }

    @Override
    public int vectorDimension() {
        return dimension;
    }

    @Override
    public Value value(long nodeId) {
        return neo4jValue(nodeId);
    }

    @Override
    public Value neo4jValue(long nodeId) {
        var value = floatArrayValue(nodeId);
        return value == null ? null : Values.float32Vector(value);
    }

    @Override
    public float[] floatArrayValue(long nodeId) {
        return internal.floatArrayValue(nodeId);
    }

    @Override
    public long nodeCount() {
        return internal.nodeCount();
    }

    @Override
    public boolean hasValue(long nodeId) {
        return internal.hasValue(nodeId);
    }
}
