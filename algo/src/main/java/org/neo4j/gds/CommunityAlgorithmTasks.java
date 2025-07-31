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
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.hdbscan.HDBScanProgressTrackerCreator;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
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

    public Task approximateMaximumKCut(Graph graph, ApproxMaxKCutParameters parameters) {
        return ApproximateKCutTaskFactory.createTask(graph, parameters);
    }

    public Task cliqueCounting(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.CliqueCounting.asString(), -1); //todo
    }

    public Task conductance(Graph graph) {
        return Tasks.task(
            AlgorithmLabel.Conductance.asString(),
            Tasks.leaf("count relationships", graph.nodeCount()),
            Tasks.leaf("accumulate counts"),
            Tasks.leaf("perform conductance computations")
        );
    }

    public Task hdbscan(Graph graph) {
        return HDBScanProgressTrackerCreator.hdbscanTask(AlgorithmLabel.HDBScan.asString(), graph.nodeCount());
    }

    public Task k1Coloring(Graph graph, K1ColoringBaseConfig configuration) {
        return K1ColoringProgressTrackerTaskCreator.progressTask(
            graph.nodeCount(),
            configuration.maxIterations()
        );
    }

    public Task kCore(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.KCore.asString(), graph.nodeCount());
    }

    public Task kMeans(Graph graph, KmeansParameters parameters) {
        return KMeansTaskFactory.createTask(graph, parameters);
    }

    public Task labelPropagation(Graph graph, LabelPropagationParameters parameters) {
        return Tasks.task(
            AlgorithmLabel.LabelPropagation.asString(),
            Tasks.leaf("Initialization", graph.relationshipCount()),
            Tasks.iterativeDynamic(
                "Assign labels",
                () -> List.of(Tasks.leaf("Iteration", graph.relationshipCount())),
                parameters.maxIterations()
            )
        );
    }

    public Task lcc(Graph graph, LocalClusteringCoefficientParameters parameters) {
        var tasks = new ArrayList<Task>();
        if (parameters.seedProperty() == null) {
            tasks.add(TriangleCountTask.create(graph.nodeCount()));
        }
        tasks.add(Tasks.leaf("Calculate Local Clustering Coefficient", graph.nodeCount()));
        return Tasks.task(AlgorithmLabel.LCC.asString(), tasks);
    }

    public Task leiden(Graph graph, LeidenParameters parameters) {
        return LeidenTask.create(graph, parameters);
    }

    public Task louvain(Graph graph, LouvainParameters parameters) {
        return LouvainProgressTrackerTaskCreator.createTask(
            graph.nodeCount(),
            graph.relationshipCount(),
            parameters.maxLevels(),
            parameters.maxIterations()
        );
    }

    public Task modularityOptimization(Graph graph, ModularityOptimizationParameters parameters) {
        return ModularityOptimizationProgressTrackerTaskCreator.progressTask(
            graph.nodeCount(),
            graph.relationshipCount(),
            parameters.maxIterations()
        );
    }

    public Task scc(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.SCC.asString(), graph.nodeCount());
    }

    public Task triangleCount(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.TriangleCount.asString(), graph.nodeCount());
    }

    public Task wcc(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.WCC.asString(), graph.relationshipCount());
    }

    public Task speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        return SpeakerListenerLPAProgressTrackerCreator.progressTask(
            graph.nodeCount(),
            configuration.maxIterations(),
            AlgorithmLabel.SLLPA.asString()
        );
    }
}
