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

import org.neo4j.gds.CommunityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingBaseConfig;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceConfigTransformer;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.hdbscan.HDBScanBaseConfig;
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;

import java.util.stream.Stream;

public class TrackedCommunityAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final CommunityAlgorithms algorithms;

    public TrackedCommunityAlgorithms(ProgressTrackerCreator progressTrackerCreator, CommunityAlgorithms algorithms) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.algorithms = algorithms;
    }

    ApproxMaxKCutResult approximateMaximumKCut(Graph graph, ApproxMaxKCutBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.approximateMaximumKCut(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.approximateMaximumKCut(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }

    CliqueCountingResult cliqueCounting(Graph graph, CliqueCountingBaseConfig configuration) {
        var params = configuration.toParameters();
        var task = CommunityAlgorithmTasks.cliqueCounting(graph, params);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.cliqueCounting(graph, params, progressTracker),
            progressTracker,
            true
        );
    }

    ConductanceResult conductance(Graph graph, ConductanceBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.conductance(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var conductanceParameters = ConductanceConfigTransformer.toParameters(configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.conductance(graph, conductanceParameters, progressTracker),
            progressTracker,
            true
        );
    }

    Labels hdbscan(Graph graph, HDBScanBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.hdbscan(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.hdbscan(graph, params, progressTracker),
            progressTracker,
            true
        );
    }

    K1ColoringResult k1Coloring(Graph graph, K1ColoringBaseConfig configuration) {
        var params = configuration.toParameters();

        var task = CommunityAlgorithmTasks.k1Coloring(graph, params);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.k1Coloring(graph, params, progressTracker),
            progressTracker,
            true
        );
    }

    KCoreDecompositionResult kCoreDecomposition(Graph graph, KCoreDecompositionBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.kCore(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.kCoreDecomposition(graph, params, progressTracker),
            progressTracker,
            true
        );
    }

    KmeansResult kMeans(Graph graph, KmeansBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.kMeans(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.kMeans(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }

    LabelPropagationResult labelPropagation(Graph graph, LabelPropagationBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.labelPropagation(graph.relationshipCount(), parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.labelPropagation(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }

    LocalClusteringCoefficientResult localClusteringCoefficient(Graph graph, LocalClusteringCoefficientBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.localClusteringCoefficient(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.localClusteringCoefficient(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }

    LeidenResult leiden(Graph graph, LeidenBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.leiden(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.leiden(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }

    LouvainResult louvain(Graph graph, LouvainBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.louvain(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.louvain(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }

    ModularityResult modularity(Graph graph, ModularityBaseConfig configuration) {
        return algorithms.modularity(graph, configuration.toParameters());
    }

    PregelResult speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        var task = CommunityAlgorithmTasks.speakerListenerLPA(graph, configuration);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.speakerListenerLPA(graph, configuration, progressTracker),
            progressTracker,
            true
        );
    }

    TriangleCountResult triangleCount(Graph graph, TriangleCountBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.triangleCount(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.triangleCount(graph, params, progressTracker),
            progressTracker,
            true
        );
    }

    Stream<TriangleResult> triangles(Graph graph, TriangleCountBaseConfig configuration) {
        return algorithms.triangles(graph, configuration.toParameters());
    }
}
