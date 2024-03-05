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
package org.neo4j.gds.undirected;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.loading.AdjacencyListBehavior;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;

public class ToUndirectedMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<ToUndirectedMemoryEstimateParameters> {

    @Override
    public MemoryEstimation memoryEstimation(ToUndirectedMemoryEstimateParameters configuration) {
        RelationshipType relationshipType = configuration.internalRelationshipType();

        var builder = MemoryEstimations.builder(ToUndirected.class)
            .add("relationships", AdjacencyListBehavior.adjacencyListEstimation(relationshipType, true));

        builder.perGraphDimension("properties", ((graphDimensions, concurrency) -> {
            long max = graphDimensions.relationshipPropertyTokens().keySet().stream().mapToLong(__ ->
                AdjacencyListBehavior
                    .adjacencyPropertiesEstimation(relationshipType, true)
                    .estimate(graphDimensions, concurrency)
                    .memoryUsage().max
            ).sum();
            return MemoryRange.of(0, max);
        }));

        return builder.build();
    }
    
}
