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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;

class GraphSageAlgorithmFactory<CONFIG extends GraphSageBaseConfig> implements AlgorithmFactory<GraphSage, CONFIG> {

    @Override
    public GraphSage build(Graph graph, CONFIG configuration, AllocationTracker tracker, Log log) {
        return new GraphSage(
            graph,
            configuration,
            ModelCatalog.get(
                configuration.username(),
                configuration.modelName(),
                ModelData.class,
                GraphSageTrainConfig.class
            ),
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var embeddingSize = configuration.embeddingSizeFromModel();
        var batchSize = configuration.batchSize();

        return MemoryEstimations
            .builder(GraphSage.class)
            .add("embeddings", HugeObjectArray.memoryEstimation(sizeOfDoubleArray(embeddingSize)))
            .add("features", HugeObjectArray.memoryEstimation(sizeOfDoubleArray(embeddingSize)))
            .perThread("batches", sizeOfDoubleArray(embeddingSize * batchSize))
            .build();
    }
}
