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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceConfigTransformer;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
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
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.SccCommonBaseConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.stream.Stream;

public class CommunityAlgorithmsBusinessFacade {

    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final CommunityAlgorithms algorithms;

    private final ProgressTrackerCreator progressTrackerCreator;

    private final CommunityAlgorithmTasks tasks = new CommunityAlgorithmTasks();

    public CommunityAlgorithmsBusinessFacade(
        CommunityAlgorithms algorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.algorithms = algorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    ApproxMaxKCutResult approximateMaximumKCut(Graph graph, ApproxMaxKCutBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.approximateMaximumKCut(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.approximateMaximumKCut(graph, parameters, progressTracker);
    }

    ConductanceResult conductance(Graph graph, ConductanceBaseConfig configuration) {
        var task = tasks.conductance(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.conductance(graph, ConductanceConfigTransformer.toParameters(configuration), progressTracker);
    }

    public Labels hdbscan(Graph graph, HDBScanBaseConfig configuration) {
        var task = tasks.hdbscan(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.hdbscan(graph, configuration.toParameters(), progressTracker);
    }

    public K1ColoringResult k1Coloring(Graph graph, K1ColoringBaseConfig configuration) {
        var task = tasks.k1Coloring(graph, configuration);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.k1Coloring(graph, configuration.toParameters(), progressTracker);
    }

    KCoreDecompositionResult kCore(Graph graph, KCoreDecompositionBaseConfig configuration) {
        var task = tasks.kCore(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.kCore(graph, configuration.toParameters(), progressTracker);
    }

    public KmeansResult kMeans(Graph graph, KmeansBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.kMeans(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.kMeans(graph, parameters, progressTracker);
    }

    LabelPropagationResult labelPropagation(Graph graph, LabelPropagationBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.labelPropagation(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.labelPropagation(graph, parameters, progressTracker);
    }

    LocalClusteringCoefficientResult lcc(Graph graph, LocalClusteringCoefficientBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.lcc(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.lcc(graph, parameters, progressTracker);
    }

    public LeidenResult leiden(Graph graph, LeidenBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.leiden(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.leiden(graph, parameters, progressTracker);
    }

    LouvainResult louvain(Graph graph, LouvainBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.louvain(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.louvain(graph, parameters, progressTracker);
    }

    ModularityResult modularity(Graph graph, ModularityBaseConfig configuration) {
        return algorithms.modularity(graph, configuration.toParameters());
    }

    ModularityOptimizationResult modularityOptimization(Graph graph, ModularityOptimizationBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.modularityOptimization(graph, parameters);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.modularityOptimization(graph, parameters, progressTracker);
    }

    HugeLongArray scc(Graph graph, SccCommonBaseConfig configuration) {
        var task = tasks.scc(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.scc(graph, configuration.toParameters(), progressTracker);
    }

    TriangleCountResult triangleCount(Graph graph, TriangleCountBaseConfig configuration) {
        var task = tasks.triangleCount(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.triangleCount(graph, configuration.toParameters(), progressTracker);
    }

    Stream<TriangleResult> triangles(Graph graph, TriangleCountBaseConfig configuration) {
        return algorithms.triangles(graph, configuration.toParameters());
    }

    public DisjointSetStruct wcc(Graph graph, WccBaseConfig configuration) {
        var task = tasks.wcc(graph);
        var progressTracker = createProgressTracker(task, configuration);

        if (configuration.hasRelationshipWeightProperty() && configuration.threshold() == 0) {
            progressTracker.logWarning(
                "Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
        }

        return algorithms.wcc(graph, configuration.toParameters(), progressTracker);
    }

    PregelResult speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        var task = tasks.speakerListenerLPA(graph, configuration);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithms.speakerListenerLPA(graph, configuration, progressTracker);
    }

    private ProgressTracker createProgressTracker(Task task, AlgoBaseConfig configuration) {
        return progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );
    }
}
