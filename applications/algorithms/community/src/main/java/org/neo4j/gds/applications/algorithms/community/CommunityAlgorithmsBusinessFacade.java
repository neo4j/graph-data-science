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
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingBaseConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceConfigTransformer;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
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
import org.neo4j.gds.logging.Log;
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
    private final Log log;
    private final CommunityAlgorithms algorithms;
    private final ProgressTrackerCreator progressTrackerCreator;

    public CommunityAlgorithmsBusinessFacade(
        Log log,
        CommunityAlgorithms algorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.log = log;
        this.algorithms = algorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    ApproxMaxKCutResult approximateMaximumKCut(Graph graph, ApproxMaxKCutBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.approximateMaximumKCut(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.approximateMaximumKCut(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    public CliqueCountingResult cliqueCounting(Graph graph, CliqueCountingBaseConfig configuration) {
        var params = configuration.toParameters();
        var task = CommunityAlgorithmTasks.cliqueCounting(graph, params);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.cliqueCounting(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    ConductanceResult conductance(Graph graph, ConductanceBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.conductance(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = ConductanceConfigTransformer.toParameters(configuration);
        return algorithmMachinery.getResult(
            () -> algorithms.conductance(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    public Labels hdbscan(Graph graph, HDBScanBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.hdbscan(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();

        return algorithmMachinery.getResult(
            () -> algorithms.hdbscan(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    public K1ColoringResult k1Coloring(Graph graph, K1ColoringBaseConfig configuration) {
        var params = configuration.toParameters();

        var task = CommunityAlgorithmTasks.k1Coloring(graph, params);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.k1Coloring(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    KCoreDecompositionResult kCore(Graph graph, KCoreDecompositionBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.kCore(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();
        return algorithmMachinery.getResult(
            () -> algorithms.kCore(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    public KmeansResult kMeans(Graph graph, KmeansBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.kMeans(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.kMeans(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    LabelPropagationResult labelPropagation(Graph graph, LabelPropagationBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.labelPropagation(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.labelPropagation(graph, parameters, progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }

    LocalClusteringCoefficientResult lcc(Graph graph, LocalClusteringCoefficientBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.lcc(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.lcc(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    public LeidenResult leiden(Graph graph, LeidenBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.leiden(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.leiden(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    LouvainResult louvain(Graph graph, LouvainBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.louvain(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.louvain(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    ModularityResult modularity(Graph graph, ModularityBaseConfig configuration) {
        return algorithms.modularity(graph, configuration.toParameters());
    }

    ModularityOptimizationResult modularityOptimization(Graph graph, ModularityOptimizationBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = CommunityAlgorithmTasks.modularityOptimization(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.modularityOptimization(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    HugeLongArray scc(Graph graph, SccCommonBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.scc(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();
        return algorithmMachinery.getResult(
            () -> algorithms.scc(graph, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    TriangleCountResult triangleCount(Graph graph, TriangleCountBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.triangleCount(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);
        var params = configuration.toParameters();

        return algorithmMachinery.getResult(
            () -> algorithms.triangleCount(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    Stream<TriangleResult> triangles(Graph graph, TriangleCountBaseConfig configuration) {
        return algorithms.triangles(graph, configuration.toParameters());
    }

    public DisjointSetStruct wcc(Graph graph, WccBaseConfig configuration) {
        var task = CommunityAlgorithmTasks.wcc(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        if (configuration.hasRelationshipWeightProperty() && configuration.threshold() == 0) {
            log.warn("Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
        }
        var params = configuration.toParameters();
        return algorithmMachinery.getResult(
            () -> algorithms.wcc(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    PregelResult speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        var task = CommunityAlgorithmTasks.speakerListenerLPA(graph, configuration);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.speakerListenerLPA(graph, configuration, progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }
}
