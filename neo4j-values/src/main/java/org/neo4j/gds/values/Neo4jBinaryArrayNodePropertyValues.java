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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.BinaryArrayNodePropertyValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Optional;

public class Neo4jBinaryArrayNodePropertyValues implements Neo4jNodePropertyValues {
    private final BinaryArrayNodePropertyValues internal;

    Neo4jBinaryArrayNodePropertyValues(BinaryArrayNodePropertyValues internal) {
        this.internal = internal;
    }

    @Override
    public Value value(long nodeId) {
        return neo4jValue(nodeId);
    }

    @Override
    public Value neo4jValue(long nodeId) {
        // as Boolean array is not an official property type in GDS we transform to double[].
        // We use the same data type as in the dense case.
        return Values.doubleArray(internal.doubleArrayValue(nodeId));
    }

    @Override
    public @Nullable Object getObject(long nodeId) {
        return internal.getObject(nodeId);
    }

    @Override
    public long nodeCount() {
        return internal.nodeCount();
    }

    @Override
    public Optional<Integer> dimension() {
        return internal.dimension();
    }

    @Override
    public ValueType valueType() {
        return internal.valueType();
    }
}
