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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.ArrayList;
import java.util.List;

public class HashGNNFactory<CONFIG extends HashGNNConfig> extends GraphAlgorithmFactory<HashGNN, CONFIG> {

    @Override
    public String taskName() {
        return "HashGNN";
    }

    @Override
    public HashGNN build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return new HashGNN(
            graph,
            configuration,
            progressTracker
        );
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        var tasks = new ArrayList<Task>();

        if (config.generateFeatures().isPresent()) {
            tasks.add(Tasks.leaf("Generate base node property features", graph.nodeCount()));
        } else if (config.binarizeFeatures().isPresent()) {
            tasks.add(Tasks.leaf("Binarize node property features", graph.nodeCount()));
        } else {
            tasks.add(Tasks.leaf("Extract raw node property features", graph.nodeCount()));
        }

        int numRelTypes = config.heterogeneous() ? config.relationshipTypes().size() : 1;

        tasks.add(Tasks.iterativeFixed(
            "Propagate embeddings",
            () -> List.of(
                Tasks.leaf(
                    "Precompute hashes",
                    config.embeddingDensity() * (1 + 1 + numRelTypes)
                ),
                Tasks.leaf(
                    "Propagate embeddings iteration",
                    (2 * graph.nodeCount() + graph.relationshipCount()) * config.embeddingDensity()
                )
            ),
            config.iterations()
        ));

        if (config.outputDimension().isPresent()) {
            tasks.add(Tasks.leaf("Densify output embeddings", graph.nodeCount()));
        }

        return Tasks.task(
            taskName(),
            tasks
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        int FUDGED_BINARY_DIMENSION = 1024;
        int binaryDimension = config
            .generateFeatures()
            .map(GenerateFeaturesConfig::dimension)
            .orElse(config.binarizeFeatures().map(BinarizeFeaturesConfig::dimension).orElse(FUDGED_BINARY_DIMENSION));

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
            config.embeddingDensity() * HashTask.Hashes.memoryEstimation(
                binaryDimension,
                config.heterogeneous() ? dims.relationshipCounts().size() : 1
            )));

        int outputDimension = config.outputDimension().orElse(FUDGED_BINARY_DIMENSION);
        builder.perNode(
            "Embeddings output",
            n -> HugeObjectArray.memoryEstimation(n, MemoryUsage.sizeOfDoubleArray(outputDimension))
        );

        return builder.build();
    }
}
