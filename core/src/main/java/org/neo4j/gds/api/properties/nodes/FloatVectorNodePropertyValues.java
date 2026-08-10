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

import java.util.Optional;

/**
 * A float array property with a known dimension for all properties.
 */
public interface FloatVectorNodePropertyValues extends VectorNodePropertyValues {

    @Override
    float[] floatArrayValue(long nodeId);

    @Override
    default ValueType valueType() {
        return ValueType.FLOAT_VECTOR;
    }

    @Override
    default Optional<Integer> dimension(long nodeId) {
        return Optional.of(vectorDimension());
    }

    @Override
    @Nullable default Object getObject(long nodeId) {
        return floatArrayValue(nodeId);
    };
}
