/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
    private final AdjacencyList inAdjacency;
    private final AdjacencyList outAdjacency;
    private final AdjacencyOffsets inOffsets;
    private final AdjacencyOffsets outOffsets;
    private final Optional<Double> maybeDefaultRelProperty;
    private final AdjacencyList inRelProperties;
    private final AdjacencyList outRelProperties;
    private final AdjacencyOffsets inRelPropertyOffsets;
    private final AdjacencyOffsets outRelPropertyOffsets;

    public Relationships(
            long rows,
            long relationshipCount,
            AdjacencyList inAdjacency,
            AdjacencyList outAdjacency,
            AdjacencyOffsets inOffsets,
            AdjacencyOffsets outOffsets,
            Optional<Double> maybeDefaultRelProperty,
            AdjacencyList inRelProperties,
            AdjacencyList outRelProperties,
            AdjacencyOffsets inRelPropertyOffsets,
            AdjacencyOffsets outRelPropertyOffsets) {
        this.rows = rows;
        this.relationshipCount = relationshipCount;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        this.maybeDefaultRelProperty = maybeDefaultRelProperty;
        this.inRelProperties = inRelProperties;
        this.outRelProperties = outRelProperties;
        this.inRelPropertyOffsets = inRelPropertyOffsets;
        this.outRelPropertyOffsets = outRelPropertyOffsets;
    }

    public long rows() {
        return rows;
    }

    public long relationshipCount() { return relationshipCount; }

    public AdjacencyList inAdjacency() { return inAdjacency; }

    public AdjacencyList outAdjacency() { return outAdjacency; }

    public AdjacencyOffsets inOffsets() { return inOffsets; }

    public AdjacencyOffsets outOffsets() { return outOffsets; }

    public Optional<Double> maybeDefaultRelProperty() { return maybeDefaultRelProperty; }

    public AdjacencyList inRelProperties() { return inRelProperties; }

    public AdjacencyList outRelProperties() { return outRelProperties; }

    public AdjacencyOffsets inRelPropertyOffsets() { return inRelPropertyOffsets; }

    public AdjacencyOffsets outRelPropertyOffsets() { return outRelPropertyOffsets; }
}
