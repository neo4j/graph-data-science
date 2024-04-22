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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.RESIDENT_MEMORY;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.TEMPORARY_MEMORY;
import static org.neo4j.gds.mem.Estimate.sizeOfDoubleArray;

public class GraphSageMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final GraphSageTrainMemoryEstimateParameters trainEstimationParameters;
    private final boolean mutating;

    public GraphSageMemoryEstimateDefinition(
        GraphSageTrainMemoryEstimateParameters trainEstimationParameters,
        boolean mutating
    ) {
        this.trainEstimationParameters = trainEstimationParameters;
        this.mutating = mutating;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup(
            "",
            graphDimensions -> memoryEstimation(graphDimensions.nodeCount())
        );
    }

    private MemoryEstimation memoryEstimation(long nodeCount) {
        var gsBuilder = MemoryEstimations.builder("GraphSage");

        if (mutating) {
            gsBuilder = gsBuilder
                .startField(RESIDENT_MEMORY)
                .perNode(
                    "resultFeatures",
                    nc -> HugeObjectArray.memoryEstimation(nc, sizeOfDoubleArray(trainEstimationParameters.embeddingDimension()))
                )
                .endField();
        }

        var builder = gsBuilder
            .startField(TEMPORARY_MEMORY)
            .field("this.instance", GraphSage.class)
            .perNode(
                "initialFeatures",
                nc -> HugeObjectArray.memoryEstimation(nc, sizeOfDoubleArray(trainEstimationParameters.estimationFeatureDimension()))
            )
            .perThread(
                "concurrentBatches",
                MemoryEstimations.builder().add(
                    GraphSageHelper.embeddingsEstimation(trainEstimationParameters, trainEstimationParameters.batchSize(), nodeCount, 0, false)
                ).build()
            );
        if (!mutating) {
            builder = builder.perNode(
                "resultFeatures",
                nc -> HugeObjectArray.memoryEstimation(nc, sizeOfDoubleArray(trainEstimationParameters.embeddingDimension()))
            );
        }
        return builder.endField().build();
    }

}
