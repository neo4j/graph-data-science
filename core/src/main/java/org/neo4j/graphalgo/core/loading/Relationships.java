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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;

import java.util.Optional;

public class Relationships {
    private final long rows;
    private final long relationshipCount;
    private final AdjacencyList adjacencyList;
    private final AdjacencyOffsets adjacencyOffsets;
    private final AdjacencyList properties;
    private final AdjacencyOffsets propertyOffsets;

    public Relationships(
        long rows,
        long relationshipCount,
        AdjacencyList adjacencyList,
        AdjacencyOffsets adjacencyOffsets,
        AdjacencyList properties,
        AdjacencyOffsets propertyOffsets
    ) {
        this.rows = rows;
        this.relationshipCount = relationshipCount;
        this.adjacencyList = adjacencyList;
        this.adjacencyOffsets = adjacencyOffsets;
        this.properties = properties;
        this.propertyOffsets = propertyOffsets;
    }

    public long rows() {
        return rows;
    }

    public long relationshipCount() { return relationshipCount; }

    public AdjacencyList adjacencyList() { return adjacencyList; }

    public AdjacencyOffsets adjacencyOffsets() { return adjacencyOffsets; }

    public AdjacencyList properties() { return properties; }

    public AdjacencyOffsets propertyOffsets() { return propertyOffsets; }
}
