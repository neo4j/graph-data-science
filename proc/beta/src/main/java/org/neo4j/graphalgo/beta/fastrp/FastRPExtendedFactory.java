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
package org.neo4j.graphalgo.beta.fastrp;

import org.neo4j.gds.beta.fastrp.FastRPExtendedBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.logging.Log;

import java.util.List;

public class FastRPExtendedFactory<CONFIG extends FastRPExtendedBaseConfig> implements AlgorithmFactory<FastRP, CONFIG> {

    @Override
    public FastRP build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, Log log,
        ProgressEventTracker eventTracker
    ) {
        var progressLogger = new BatchingProgressLogger(log, graph.nodeCount(), "FastRPE", configuration.concurrency(), eventTracker);
        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger);
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, configuration.featureProperties());

        return new FastRP(
            graph,
            configuration,
            featureExtractors,
            progressTracker,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return FastRP.memoryEstimation(configuration);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            "FastRPExtended",
            Tasks.leaf("Initialize Random Vectors", graph.nodeCount()),
            Tasks.iterativeFixed(
                "Propagate embeddings",
                () -> List.of(Tasks.leaf("Propagate embeddings task", graph.relationshipCount())),
                config.iterationWeights().size()
            )
        );
    }
}
