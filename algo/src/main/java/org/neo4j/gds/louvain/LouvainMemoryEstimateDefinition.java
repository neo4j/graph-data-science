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
package org.neo4j.gds.louvain;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMemoryEstimateDefinition;

public class LouvainMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<LouvainMemoryEstimationParameters> {

    @Override
    public MemoryEstimation memoryEstimation(LouvainMemoryEstimationParameters parameters) {
        int maxLevels = parameters.maxLevels();
        return MemoryEstimations.builder(Louvain.class)
            .add(
                "modularityOptimization()",
                new ModularityOptimizationMemoryEstimateDefinition().memoryEstimation(null)
            )
            .rangePerGraphDimension("subGraph", (graphDimensions, concurrency) -> {
                ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions
                    .builder()
                    .from(graphDimensions);

                GraphDimensions sparseDimensions = dimensionsBuilder.build();

                // Louvain creates a new graph every iteration, this graph has one relationship property
                RelationshipProjections relationshipProjections = ImmutableRelationshipProjections.builder()
                    .putProjection(
                        RelationshipType.of("AGGREGATE"),
                        RelationshipProjection.builder()
                            .type("AGGREGATE")
                            .orientation(Orientation.UNDIRECTED)
                            .aggregation(Aggregation.SUM)
                            .addProperty("prop", "prop", DefaultValue.of(0.0))
                            .build()
                    )
                    .build();

                long maxGraphSize = CSRGraphStoreFactory
                    .getMemoryEstimation(NodeProjections.all(), relationshipProjections, false)
                    .estimate(sparseDimensions, concurrency)
                    .memoryUsage()
                    .max;

                return MemoryRange.of(1L, maxGraphSize); // rough estimate of graph size
            })
            .rangePerNode("dendrograms", (nodeCount) -> MemoryRange.of(
                HugeLongArray.memoryEstimation(nodeCount),
                HugeLongArray.memoryEstimation(nodeCount) * (parameters.includeIntermediateCommunities()
                    ? maxLevels : Math.min(2, maxLevels))
            ))
            .build();
    }

}
