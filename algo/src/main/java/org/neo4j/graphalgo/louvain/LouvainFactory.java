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
package org.neo4j.graphalgo.louvain;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.modularity.ModularityOptimizationFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.logging.Log;

import java.util.Collections;

public class LouvainFactory<CONFIG extends LouvainBaseConfig> extends AlgorithmFactory<Louvain, CONFIG> {

    @Override
    public Louvain build(
        final Graph graph,
        final LouvainBaseConfig configuration,
        final AllocationTracker tracker,
        final Log log
    ) {
        BatchingProgressLogger progressLogger = new BatchingProgressLogger(log, 1, "Louvain");

        return new Louvain(
            graph,
            configuration,
            Pools.DEFAULT,
            progressLogger,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return MemoryEstimations.builder(Louvain.class)
            .add("modularityOptimization()", ModularityOptimizationFactory.MEMORY_ESTIMATION)
            .rangePerGraphDimension("subGraph", (graphDimensions, concurrency) -> {
                ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions.builder().from(graphDimensions);

                graphDimensions
                    .relationshipProperties()
                    .stream()
                    .findFirst()
                    .map(prop -> dimensionsBuilder.relationshipProperties(ResolvedPropertyMappings.of(Collections.singletonList(prop))));

                dimensionsBuilder.nodePropertyIds(graphDimensions.nodePropertyIds());

                GraphDimensions sparseDimensions = dimensionsBuilder.build();

                long maxGraphSize = NativeFactory
                    .getMemoryEstimation(sparseDimensions)
                    .estimate(sparseDimensions, concurrency)
                    .memoryUsage()
                    .max;
                return MemoryRange.of(1L, maxGraphSize); // rough estimate of graph size
            })
            .rangePerNode("dendrograms", (nodeCount) -> MemoryRange.of(
                HugeLongArray.memoryEstimation(nodeCount),
                HugeLongArray.memoryEstimation(nodeCount) * config.maxLevels()
            ))
            .build();
    }
}
