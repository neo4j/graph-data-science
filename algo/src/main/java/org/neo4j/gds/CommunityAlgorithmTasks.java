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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.approxmaxkcut.ApproximateKCutTaskFactory;
import org.neo4j.gds.cliqueCounting.CliqueCountingTaskFactory;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.hdbscan.HDBScanProgressTrackerCreator;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.k1coloring.K1ColoringProgressTrackerTaskCreator;
import org.neo4j.gds.kmeans.KMeansTaskFactory;
import org.neo4j.gds.kmeans.KmeansParameters;
import org.neo4j.gds.labelpropagation.LabelPropagationParameters;
import org.neo4j.gds.leiden.LeidenParameters;
import org.neo4j.gds.leiden.LeidenTask;
import org.neo4j.gds.louvain.LouvainParameters;
import org.neo4j.gds.louvain.LouvainProgressTrackerTaskCreator;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationParameters;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationProgressTrackerTaskCreator;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAProgressTrackerCreator;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;
import org.neo4j.gds.triangle.TriangleCountTask;

import java.util.ArrayList;
import java.util.List;

public final class CommunityAlgorithmTasks {
    private CommunityAlgorithmTasks() {}

    public static Task approximateMaximumKCut(Graph graph, ApproxMaxKCutParameters parameters) {
        return ApproximateKCutTaskFactory.createTask(graph, parameters);
    }

    public static Task cliqueCounting(Graph graph, CliqueCountingParameters parameters) {
        return CliqueCountingTaskFactory.createTask(graph, parameters);
    }

    public static Task conductance(Graph graph, Concurrency concurrency) {
        return Tasks.task(
            AlgorithmLabel.Conductance.asString(),
            concurrency,
            Tasks.leaf("count relationships", concurrency, graph.nodeCount()),
            Tasks.leaf("accumulate counts", concurrency),
            Tasks.leaf("perform conductance computations", concurrency)
        );
    }

    public static Task hdbscan(Graph graph, Concurrency concurrency) {
        return HDBScanProgressTrackerCreator.hdbscanTask(
            AlgorithmLabel.HDBScan.asString(),
            concurrency,
            graph.nodeCount()
        );
    }

    public static Task k1Coloring(Graph graph, K1ColoringParameters parameters) {
        return K1ColoringProgressTrackerTaskCreator.progressTask(
            parameters.concurrency(),
            graph.nodeCount(),
            parameters.maxIterations()
        );
    }

    public static Task kCore(Graph graph, Concurrency concurrency) {
        return Tasks.leaf(AlgorithmLabel.KCore.asString(), concurrency, graph.nodeCount());
    }

    public static Task kMeans(Graph graph, KmeansParameters parameters) {
        return KMeansTaskFactory.createTask(graph, parameters);
    }

    public static Task labelPropagation(Graph graph, LabelPropagationParameters parameters) {
        return Tasks.task(
            AlgorithmLabel.LabelPropagation.asString(),
            parameters.concurrency(),
            Tasks.leaf("Initialization", parameters.concurrency(), graph.relationshipCount()),
            Tasks.iterativeDynamic(
                "Assign labels",
                parameters.concurrency(),
                () -> List.of(Tasks.leaf("Iteration", parameters.concurrency(), graph.relationshipCount())),
                parameters.maxIterations()
            )
        );
    }

    public static Task lcc(Graph graph, LocalClusteringCoefficientParameters parameters) {
        var tasks = new ArrayList<Task>();
        if (parameters.seedProperty() == null) {
            tasks.add(TriangleCountTask.create(parameters.concurrency(), graph.nodeCount()));
        }
        tasks.add(Tasks.leaf("Calculate Local Clustering Coefficient", parameters.concurrency(), graph.nodeCount()));
        return Tasks.task(AlgorithmLabel.LCC.asString(), parameters.concurrency(), tasks);
    }

    public static Task leiden(Graph graph, LeidenParameters parameters) {
        return LeidenTask.create(graph, parameters);
    }

    public static Task louvain(Graph graph, LouvainParameters parameters) {
        return LouvainProgressTrackerTaskCreator.createTask(
            parameters.concurrency(),
            graph.nodeCount(),
            graph.relationshipCount(),
            parameters.maxLevels(),
            parameters.maxIterations()
        );
    }

    public static Task modularityOptimization(Graph graph, ModularityOptimizationParameters parameters) {
        return ModularityOptimizationProgressTrackerTaskCreator.progressTask(
            parameters.concurrency(),
            graph.nodeCount(),
            graph.relationshipCount(),
            parameters.maxIterations()
        );
    }

    public static Task scc(Graph graph, Concurrency concurrency) {
        return Tasks.leaf(AlgorithmLabel.SCC.asString(), concurrency, graph.nodeCount());
    }

    public static Task triangleCount(Graph graph, Concurrency concurrency) {
        return Tasks.leaf(AlgorithmLabel.TriangleCount.asString(), concurrency, graph.nodeCount());
    }

    public static Task wcc(Graph graph, Concurrency concurrency) {
        return Tasks.leaf(AlgorithmLabel.WCC.asString(), concurrency, graph.relationshipCount());
    }

    public static Task speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        return SpeakerListenerLPAProgressTrackerCreator.progressTask(
            AlgorithmLabel.SLLPA.asString(),
            configuration.concurrency(),
            graph.nodeCount(),
            configuration.maxIterations()
        );
    }
}
