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
package org.neo4j.gds.core.utils;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;

import java.util.OptionalLong;
import java.util.function.LongUnaryOperator;

public class OriginalIdNodePropertyValues implements LongNodePropertyValues {
    private final LongUnaryOperator toOriginalNodeId;
    private final long nodeCount;
    private final OptionalLong maxOriginalId;

    public OriginalIdNodePropertyValues(IdMap idMap) {
        this(
            idMap::toOriginalNodeId,
            idMap.nodeCount(),
            OptionalLong.of(idMap.highestOriginalId())
        );
    }

    public OriginalIdNodePropertyValues(
        LongUnaryOperator toOriginalNodeId,
        long nodeCount,
        OptionalLong maxOriginalId
    ) {
        this.toOriginalNodeId = toOriginalNodeId;
        this.nodeCount = nodeCount;
        this.maxOriginalId = maxOriginalId;
    }

    @Override
    public long longValue(long nodeId) {
        return toOriginalNodeId.applyAsLong(nodeId);
    }

    @Override
    public OptionalLong getMaxLongPropertyValue() {
        return maxOriginalId;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }
}
