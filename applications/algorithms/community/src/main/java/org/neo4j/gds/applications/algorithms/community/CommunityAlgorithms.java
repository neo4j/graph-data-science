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
import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.hdbscan.HDBScan;
import org.neo4j.gds.hdbscan.HDBScanBaseConfig;
import org.neo4j.gds.hdbscan.HDBScanParameters;
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecomposition;
import org.neo4j.gds.kcore.KCoreDecompositionParameters;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.Kmeans;
import org.neo4j.gds.kmeans.KmeansContext;
import org.neo4j.gds.kmeans.KmeansParameters;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagation;
import org.neo4j.gds.labelpropagation.LabelPropagationParameters;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.Leiden;
import org.neo4j.gds.leiden.LeidenParameters;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.Louvain;
import org.neo4j.gds.louvain.LouvainParameters;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityCalculator;
import org.neo4j.gds.modularity.ModularityParameters;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularityoptimization.K1ColoringStub;
import org.neo4j.gds.modularityoptimization.ModularityOptimization;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationParameters;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.Scc;
import org.neo4j.gds.scc.SccCommonBaseConfig;
import org.neo4j.gds.scc.SccParameters;
import org.neo4j.gds.sllpa.SpeakerListenerLPA;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.IntersectingTriangleCount;
import org.neo4j.gds.triangle.LocalClusteringCoefficient;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountParameters;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.triangle.TriangleStream;
import org.neo4j.gds.wcc.WccBaseConfig;
import org.neo4j.gds.wcc.WccParameters;
import org.neo4j.gds.wcc.WccStub;

import java.util.Optional;
import java.util.stream.Stream;

public class CommunityAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    private final CommunityAlgorithmTasks tasks = new CommunityAlgorithmTasks();

    public CommunityAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    ApproxMaxKCutResult approximateMaximumKCut(Graph graph, ApproxMaxKCutBaseConfig configuration) {
        var task = tasks.approximateMaximumKCut(graph, configuration.toParameters());
        var progressTracker = createProgressTracker(task, configuration);

        return approximateMaximumKCut(graph, configuration.toParameters(), progressTracker);
    }

    public ApproxMaxKCutResult approximateMaximumKCut(
        Graph graph,
        ApproxMaxKCutParameters parameters,
        ProgressTracker progressTracker
    ) {
        var algorithm = ApproxMaxKCut.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    ConductanceResult conductance(Graph graph, ConductanceParameters parameters, ProgressTracker progressTracker) {

        var algorithm = new Conductance(
            graph,
            parameters.concurrency(),
            parameters.minBatchSize(),
            parameters.hasRelationshipWeightProperty(),
            parameters.communityProperty(),
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    Labels hdbscan(Graph graph, HDBScanBaseConfig configuration) {
        var task = tasks.hdbscan(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return hdbscan(graph, configuration.toParameters(), progressTracker);
    }

    public Labels hdbscan(Graph graph, HDBScanParameters parameters, ProgressTracker progressTracker) {
        var hdbScan = new HDBScan(
            graph,
            graph.nodeProperties(parameters.nodeProperty()),
            parameters,
            progressTracker,
            terminationFlag
        );

        return hdbScan.compute();
    }

    public K1ColoringResult k1Coloring(Graph graph, K1ColoringParameters parameters, ProgressTracker progressTracker) {

        var k1ColoringStub = new K1ColoringStub(algorithmMachinery);

        return k1ColoringStub.k1Coloring(
            graph,
            parameters,
            progressTracker,
            terminationFlag,
            parameters.concurrency(),
            true
        );
    }

    KCoreDecompositionResult kCore(
        Graph graph,
        KCoreDecompositionParameters parameters,
        ProgressTracker progressTracker
    ) {
        var algorithm = new KCoreDecomposition(graph, parameters.concurrency(), progressTracker, terminationFlag);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    public KmeansResult kMeans(Graph graph, KmeansParameters parameters, ProgressTracker progressTracker) {

        var algorithm = Kmeans.createKmeans(
            graph,
            parameters,
            new KmeansContext(DefaultPool.INSTANCE, progressTracker),
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    LabelPropagationResult labelPropagation(
        Graph graph,
        LabelPropagationParameters parameters,
        ProgressTracker progressTracker
    ) {

        var algorithm = new LabelPropagation(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    LocalClusteringCoefficientResult lcc(
        Graph graph,
        LocalClusteringCoefficientParameters parameters,
        ProgressTracker progressTracker
    ) {
        var algorithm = new LocalClusteringCoefficient(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            parameters.seedProperty(),
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    public LeidenResult leiden(Graph graph, LeidenParameters parameters, ProgressTracker progressTracker) {
        var seedValues = Optional.ofNullable(parameters.seedProperty())
            .map(seedParameter -> CommunityCompanion.extractSeedingNodePropertyValues(graph, seedParameter))
            .orElse(null);

        var algorithm = new Leiden(
            graph,
            parameters,
            seedValues,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    LouvainResult louvain(Graph graph, LouvainParameters parameters, ProgressTracker progressTracker) {

        var algorithm = new Louvain(
            graph,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    ModularityResult modularity(Graph graph, ModularityBaseConfig configuration) {
        return modularity(graph, configuration.toParameters());
    }

    ModularityResult modularity(Graph graph, ModularityParameters parameters) {
        var algorithm = ModularityCalculator.create(
            graph,
            graph.nodeProperties(parameters.communityProperty())::longValue,
            parameters.concurrency()
        );

        return algorithm.compute();
    }

    ModularityOptimizationResult modularityOptimization(
        Graph graph,
        ModularityOptimizationParameters parameters,
        ProgressTracker progressTracker
    ) {

        var seedPropertyValues = parameters.seedProperty()
            .map(seedProperty ->
                CommunityCompanion.extractSeedingNodePropertyValues(
                    graph,
                    seedProperty
                )
            )
            .orElse(null);

        var algorithm = new ModularityOptimization(
            graph,
            parameters.maxIterations(),
            parameters.tolerance(),
            seedPropertyValues,
            parameters.concurrency(),
            parameters.batchSize(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    HugeLongArray scc(Graph graph, SccCommonBaseConfig configuration) {
        var task = tasks.scc(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return scc(graph, configuration.toParameters(), progressTracker);
    }

    public HugeLongArray scc(Graph graph, SccParameters parameters, ProgressTracker progressTracker) {
        var algorithm = new Scc(graph, progressTracker, terminationFlag);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    TriangleCountResult triangleCount(Graph graph, TriangleCountBaseConfig configuration) {
        var task = tasks.triangleCount(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return triangleCount(graph, configuration.toParameters(), progressTracker);
    }

    public TriangleCountResult triangleCount(
        Graph graph,
        TriangleCountParameters parameters,
        ProgressTracker progressTracker
    ) {

        var algorithm = IntersectingTriangleCount.create(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    Stream<TriangleResult> triangles(Graph graph, TriangleCountBaseConfig configuration) {
        return triangles(graph, configuration.toParameters());
    }

    Stream<TriangleResult> triangles(Graph graph, TriangleCountParameters parameters) {
        var algorithm = TriangleStream.create(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            terminationFlag
        );

        return algorithm.compute();
    }

    public DisjointSetStruct wcc(Graph graph, WccBaseConfig configuration) {
        var task = tasks.wcc(graph);
        var progressTracker = createProgressTracker(task, configuration);

        if (configuration.hasRelationshipWeightProperty() && configuration.threshold() == 0) {
            progressTracker.logWarning(
                "Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
        }

        return wcc(graph, configuration.toParameters(), progressTracker);
    }

    public DisjointSetStruct wcc(
        Graph graph,
        WccParameters parameters,
        ProgressTracker progressTracker
    ) {
        var wccStub = new WccStub(terminationFlag, algorithmMachinery);
        return wccStub.wcc(graph, parameters, progressTracker, true);
    }

    PregelResult speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration) {
        var task = tasks.speakerListenerLPA(graph, configuration);
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = new SpeakerListenerLPA(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker,
            Optional.empty()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
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
