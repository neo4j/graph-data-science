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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.mem.Estimate;

import java.util.function.LongUnaryOperator;

public class HashGNNMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final HashGNNParameters parameters;

    public HashGNNMemoryEstimateDefinition(HashGNNParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var embeddingDensity = parameters.embeddingDensity();
        var heterogeneous = parameters.heterogeneous();
        var outputDimension = parameters.outputDimension();
        var generateFeatures = parameters.generateFeatures();
        var binarizeFeatures = parameters.binarizeFeatures();
        int FUDGED_BINARY_DIMENSION = 1024;
        int binaryDimension = generateFeatures.map(GenerateFeaturesConfig::dimension)
            .orElse(binarizeFeatures.map(BinarizeFeaturesConfig::dimension).orElse(FUDGED_BINARY_DIMENSION));

        MemoryEstimations.Builder builder = MemoryEstimations.builder(HashGNN.class.getSimpleName());

        builder.perNode(
            "Embeddings cache 1",
            n -> HugeObjectArray.memoryEstimation(n, HugeAtomicBitSet.memoryEstimation(binaryDimension))
        );
        builder.perNode(
            "Embeddings cache 2",
            n -> HugeObjectArray.memoryEstimation(n, HugeAtomicBitSet.memoryEstimation(binaryDimension))
        );

        builder.perGraphDimension("Hashes cache", (dims, concurrency) -> MemoryRange.of(
            embeddingDensity * HashTask.Hashes.memoryEstimation(
                binaryDimension,
                heterogeneous ? dims.relationshipCounts().size() : 1
            )));

        LongUnaryOperator denseResultEstimation = n -> HugeObjectArray.memoryEstimation(
            n,
            Estimate.sizeOfDoubleArray(outputDimension.orElse(binaryDimension))
        );

        if (outputDimension.isPresent()) {
            builder.perNode("Embeddings output", denseResultEstimation);
        } else {
            // in the sparse case we store the bitset, but we convert the result to double[] before returning to the user
            builder.rangePerNode("Embeddings output", n -> MemoryRange.of(
                HugeObjectArray.memoryEstimation(n, Estimate.sizeOfBitset(binaryDimension)),
                denseResultEstimation.applyAsLong(n)
            ));
        }
        return builder.build();
    }

}
