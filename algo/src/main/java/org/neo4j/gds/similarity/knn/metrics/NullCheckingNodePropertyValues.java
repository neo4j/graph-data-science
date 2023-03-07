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
package org.neo4j.gds.similarity.knn.metrics;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.values.storable.Value;

import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NullCheckingNodePropertyValues implements NodePropertyValues {

    public static NullCheckingNodePropertyValues create(NodePropertyValues properties, String name, IdMap idMap) {
        var valueType = properties.valueType();
        assert valueType != ValueType.DOUBLE && valueType != ValueType.LONG: "Don't use NullCheckingNodeProperties for primitive properties";
        return new NullCheckingNodePropertyValues(properties, name, idMap);
    }

    private final NodePropertyValues properties;
    private final String name;
    private final IdMap idMap;

    private NullCheckingNodePropertyValues(NodePropertyValues properties, String name, IdMap idMap) {
        this.properties = properties;
        this.name = name;
        this.idMap = idMap;
    }

    @Override
    public double[] doubleArrayValue(long nodeId) {
        var value = properties.doubleArrayValue(nodeId);
        check(nodeId, value);
        return value;
    }

    @Override
    public float[] floatArrayValue(long nodeId) {
        var value = properties.floatArrayValue(nodeId);
        check(nodeId, value);
        return value;
    }

    @Override
    public long[] longArrayValue(long nodeId) {
        var value = properties.longArrayValue(nodeId);
        check(nodeId, value);
        return value;
    }

    @Override
    public Object getObject(long nodeId) {
        var value = properties.getObject(nodeId);
        check(nodeId, value);
        return value;
    }

    @Override
    public ValueType valueType() {
        return properties.valueType();
    }

    @Override
    public Optional<Integer> dimension() {
        return properties.dimension();
    }

    @Override
    public Value value(long nodeId) {
        var value = properties.value(nodeId);
        check(nodeId, value);
        return value;
    }

    @Override
    public long nodeCount() {
        return properties.nodeCount();
    }

    private void check(long nodeId, @Nullable Object value) {
        if (value == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Missing `%s` node property `%s` for node with id `%s`.",
                properties.valueType().cypherName(),
                name,
                idMap.toOriginalNodeId(nodeId)
            ));
        }
    }
}
