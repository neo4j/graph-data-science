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
package org.neo4j.gds.applications.graphstorecatalog;

import java.util.Map;

public class GraphGenerationStats {
    public String name;
    public long nodes;
    public long relationships;
    @SuppressWarnings("WeakerAccess")
    public long generateMillis;
    @SuppressWarnings("WeakerAccess")
    public Long relationshipSeed;
    public double averageDegree;
    public Object relationshipDistribution;
    public Object relationshipProperty;

    GraphGenerationStats(
        String graphName,
        double averageDegree,
        String relationshipDistribution,
        Map<String, Object> relationshipProperty,
        Long relationshipSeed
    ) {
        this.name = graphName;
        this.averageDegree = averageDegree;
        this.relationshipDistribution = relationshipDistribution;
        this.relationshipProperty = relationshipProperty;
        this.relationshipSeed = relationshipSeed;
    }
}
