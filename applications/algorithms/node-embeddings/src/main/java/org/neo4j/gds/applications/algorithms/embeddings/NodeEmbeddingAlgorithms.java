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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.List;

public class NodeEmbeddingAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public NodeEmbeddingAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    FastRPResult fastRP(Graph graph, FastRPBaseConfig configuration) {
        var task = progressTask(graph, configuration.nodeSelfInfluence(), configuration.iterationWeights().size());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var parameters = configuration.toParameters();

        var featureExtractors = FeatureExtraction.propertyExtractors(graph, parameters.featureProperties());

        var algorithm = new FastRP(
            graph,
            parameters,
            configuration.concurrency(),
            10_000,
            featureExtractors,
            progressTracker,
            configuration.randomSeed(),
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    private Task progressTask(Graph graph, Number nodeSelfInfluence, int iterationWeightsSize) {
        var tasks = new ArrayList<Task>();
        tasks.add(Tasks.leaf("Initialize random vectors", graph.nodeCount()));
        if (Float.compare(nodeSelfInfluence.floatValue(), 0.0f) != 0) {
            tasks.add(Tasks.leaf("Apply node self-influence", graph.nodeCount()));
        }
        tasks.add(Tasks.iterativeFixed(
            "Propagate embeddings",
            () -> List.of(Tasks.leaf("Propagate embeddings task", graph.relationshipCount())),
            iterationWeightsSize
        ));
        return Tasks.task(LabelForProgressTracking.FastRP.value, tasks);
    }
}
