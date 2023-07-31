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
package org.neo4j.gds.core.compression.mixed;

import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.PropertyCursor;

public class MixedAdjacencyProperties implements AdjacencyProperties {

    private final AdjacencyList adjacencyList;

    private final AdjacencyProperties packedAdjacencyProperties;
    private final AdjacencyProperties vlongAdjacencyProperties;

    MixedAdjacencyProperties(
        AdjacencyList adjacencyList,
        AdjacencyProperties packedAdjacencyProperties,
        AdjacencyProperties vlongAdjacencyProperties
    ) {
        this.adjacencyList = adjacencyList;
        this.packedAdjacencyProperties = packedAdjacencyProperties;
        this.vlongAdjacencyProperties = vlongAdjacencyProperties;
    }

    @Override
    public PropertyCursor propertyCursor(long node, double fallbackValue) {
        if (MixedCompressor.usePacking(this.adjacencyList.degree(node))) {
            return this.packedAdjacencyProperties.propertyCursor(node, fallbackValue);
        }
        return this.vlongAdjacencyProperties.propertyCursor(node, fallbackValue);
    }

    @Override
    public PropertyCursor propertyCursor(PropertyCursor reuse, long node, double fallbackValue) {
        if (MixedCompressor.usePacking(this.adjacencyList.degree(node))) {
            return this.packedAdjacencyProperties.propertyCursor(node, fallbackValue);
        }
        return this.vlongAdjacencyProperties.propertyCursor(reuse, node, fallbackValue);
    }

    @Override
    public PropertyCursor rawPropertyCursor() {
        return this.vlongAdjacencyProperties.rawPropertyCursor();
    }
}
