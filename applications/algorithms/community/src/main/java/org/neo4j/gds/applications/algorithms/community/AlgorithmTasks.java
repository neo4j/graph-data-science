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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.hdbscan.HDBScanProgressTrackerCreator;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringProgressTrackerTaskCreator;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainProgressTrackerTaskCreator;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationProgressTrackerTaskCreator;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAProgressTrackerCreator;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.TriangleCountTask;

import java.util.ArrayList;
import java.util.List;

class AlgorithmTasks {

    Task approximateMaximumKCut(Graph graph, ApproxMaxKCutBaseConfig configuration) {
        return ApproximateKCutTaskFactory.createTask(graph, configuration);
    }

    Task conductance(Graph graph) {
        return Tasks.task(
            AlgorithmLabel.Conductance.asString(),
            Tasks.leaf("count relationships", graph.nodeCount()),
            Tasks.leaf("accumulate counts"),
            Tasks.leaf("perform conductance computations")
        );
    }

    Task hdbscan(Graph graph) {
        return HDBScanProgressTrackerCreator.hdbscanTask(AlgorithmLabel.HDBScan.asString(), graph.nodeCount());
    }

    Task k1Coloring(Graph graph, K1ColoringBaseConfig configuration) {
        return K1ColoringProgressTrackerTaskCreator.progressTask(
            graph.nodeCount(),
            configuration.maxIterations()
        );
    }

    Task kCore(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.KCore.asString(), graph.nodeCount());
    }

    Task kMeans(Graph graph, KmeansBaseConfig configuration) {
        return KMeansTaskFactory.createTask(graph, configuration);
    }

    Task labelPropagation(Graph graph, LabelPropagationBaseConfig configuration) {
        return Tasks.task(
            AlgorithmLabel.LabelPropagation.asString(),
            Tasks.leaf("Initialization", graph.relationshipCount()),
            Tasks.iterativeDynamic(
                "Assign labels",
                () -> List.of(Tasks.leaf("Iteration", graph.relationshipCount())),
                configuration.maxIterations()
            )
        );
    }

    Task lcc(Graph graph, LocalClusteringCoefficientBaseConfig configuration) {
        var tasks = new ArrayList<Task>();
        if (configuration.seedProperty() == null) {
            tasks.add(TriangleCountTask.create(graph.nodeCount()));
        }
        tasks.add(Tasks.leaf("Calculate Local Clustering Coefficient", graph.nodeCount()));
        return Tasks.task(AlgorithmLabel.LCC.asString(), tasks);
    }

    Task leiden(Graph graph, LeidenBaseConfig configuration) {
        return LeidenTask.create(graph, configuration);
    }

    Task louvain(Graph graph, LouvainBaseConfig configuration) {
        return LouvainProgressTrackerTaskCreator.createTask(
            graph.nodeCount(),
            graph.relationshipCount(),
            configuration.maxLevels(),
            configuration.maxIterations()
        );
    }

    Task modularityOptimization(Graph graph, ModularityOptimizationBaseConfig configuration) {
        return ModularityOptimizationProgressTrackerTaskCreator.progressTask(
            graph.nodeCount(),
            graph.relationshipCount(),
            configuration.maxIterations()
        );
    }

    Task scc(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.SCC.asString(), graph.nodeCount());
    }

    Task triangleCount(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.TriangleCount.asString(), graph.nodeCount());
    }

    Task wcc(Graph graph) {
        return Tasks.leaf(AlgorithmLabel.WCC.asString(), graph.relationshipCount());
    }

    Task speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        return SpeakerListenerLPAProgressTrackerCreator.progressTask(
            graph.nodeCount(),
            configuration.maxIterations(),
            AlgorithmLabel.SLLPA.asString()
        );
    }
}
