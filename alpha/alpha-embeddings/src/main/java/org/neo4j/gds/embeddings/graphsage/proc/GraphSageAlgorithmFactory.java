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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.MutateConfig;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import static org.neo4j.graphalgo.core.utils.mem.MemoryEstimations.PERSISTENT;
import static org.neo4j.graphalgo.core.utils.mem.MemoryEstimations.TEMPORARY;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;

// TODO: move it to algo package
public class GraphSageAlgorithmFactory<CONFIG extends GraphSageBaseConfig> extends AbstractAlgorithmFactory<GraphSage, CONFIG> {

    public GraphSageAlgorithmFactory() {
        super();
    }

    @Override
    protected long taskVolume(Graph graph, CONFIG configuration) {
        return ParallelUtil.threadCount(configuration.batchSize(), graph.nodeCount());
    }

    @Override
    protected String taskName() {
        return GraphSage.class.getSimpleName();
    }

    @Override
    public GraphSage build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        return new GraphSage(
            graph,
            configuration,
            ModelCatalog.get(
                configuration.username(),
                configuration.modelName(),
                ModelData.class,
                GraphSageTrainConfig.class
            ),
            tracker,
            progressLogger
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return MemoryEstimations.setup(
            "",
            graphDimensions -> withNodeCount(
                config.trainConfig(),
                graphDimensions.nodeCount(),
                config instanceof MutateConfig
            )
        );
    }

    private MemoryEstimation withNodeCount(GraphSageTrainConfig config, long nodeCount, boolean mutate) {
        var gsBuilder = MemoryEstimations.builder("GraphSage");

        if (mutate) {
            gsBuilder = gsBuilder.startField(PERSISTENT)
                .add(
                    "resultFeatures",
                    HugeObjectArray.memoryEstimation(sizeOfDoubleArray(config.embeddingDimension()))
                ).endField();
        }

        var builder = gsBuilder
            .startField(TEMPORARY)
            .field("this.instance", GraphSage.class)
            .add(
                "initialFeatures",
                HugeObjectArray.memoryEstimation(sizeOfDoubleArray(config.featuresSize()))
            )
            .perThread(
                "concurrentBatches",
                MemoryEstimations.builder().add(
                    GraphSageHelper.embeddingsEstimation(config, config.batchSize(), nodeCount, false)
                ).build()
            );
        if (!mutate) {
            builder = builder.add(
                "resultFeatures",
                HugeObjectArray.memoryEstimation(sizeOfDoubleArray(config.embeddingDimension()))
            );
        }
        return builder.endField().build();
    }

    @TestOnly
    public GraphSageAlgorithmFactory(ProgressLogger.ProgressLoggerFactory loggerFactory) {
        super(loggerFactory);
    }
}
