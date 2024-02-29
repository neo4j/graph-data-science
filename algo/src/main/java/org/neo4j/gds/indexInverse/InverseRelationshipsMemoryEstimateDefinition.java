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
package org.neo4j.gds.indexInverse;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.loading.AdjacencyListBehavior;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

import java.util.Locale;

public class InverseRelationshipsMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<Iterable<String>> {

    @Override
    public MemoryEstimation memoryEstimation(Iterable<String> relationshipTypes) {

        var builder = MemoryEstimations.builder(InverseRelationships.class);

        for (String typeName : relationshipTypes) {
            var builderForType = MemoryEstimations.builder();
            if (typeName.equals(ElementProjection.PROJECT_ALL)) {
                builderForType.add(
                    "All relationships",
                    AdjacencyListBehavior.adjacencyListsFromStarEstimation(false)
                );

                builderForType.perGraphDimension("All properties", ((graphDimensions, concurrency) -> {
                    var singlePropertyEstimation = AdjacencyListBehavior
                        .adjacencyPropertiesFromStarEstimation( false)
                        .estimate(graphDimensions, concurrency)
                        .memoryUsage();

                    return singlePropertyEstimation.times(graphDimensions.relationshipPropertyTokens().size());
                }));

                builder.add(String.format(Locale.US, "Inverse '%s'", typeName), builderForType.build());
            } else {
                var relationshipType = RelationshipType.of(typeName);

                builderForType.add(
                    "relationships",
                    AdjacencyListBehavior.adjacencyListEstimation(relationshipType, false)
                );


                builderForType.perGraphDimension("properties", ((graphDimensions, concurrency) -> {
                    var singlePropertyEstimation = AdjacencyListBehavior
                        .adjacencyPropertiesEstimation(relationshipType, false)
                        .estimate(graphDimensions, concurrency)
                        .memoryUsage();

                    return singlePropertyEstimation.times(graphDimensions.relationshipPropertyTokens().size());
                }));

                builder.add(String.format(Locale.US, "Inverse '%s'", typeName), builderForType.build());
            }
        }

        return builder.build();
    }

}
