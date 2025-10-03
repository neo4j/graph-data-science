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

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCounting;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.hdbscan.HDBScan;
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
import org.neo4j.gds.modularity.ModularityCalculator;
import org.neo4j.gds.modularity.ModularityParameters;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularityoptimization.K1ColoringStub;
import org.neo4j.gds.modularityoptimization.ModularityOptimization;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationParameters;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.Scc;
import org.neo4j.gds.scc.SccParameters;
import org.neo4j.gds.sllpa.SpeakerListenerLPA;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.IntersectingTriangleCount;
import org.neo4j.gds.triangle.LocalClusteringCoefficient;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountParameters;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.triangle.TriangleStream;
import org.neo4j.gds.wcc.WccParameters;
import org.neo4j.gds.wcc.WccStub;

import java.util.Optional;
import java.util.stream.Stream;

public class CommunityAlgorithms {
    private final TerminationFlag terminationFlag;

    public CommunityAlgorithms(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
    }

    public ApproxMaxKCutResult approximateMaximumKCut(
        Graph graph,
        ApproxMaxKCutParameters parameters,
        ProgressTracker progressTracker
    ) {
        return ApproxMaxKCut.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        ).compute();
    }

    public CliqueCountingResult cliqueCounting(Graph graph, CliqueCountingParameters parameters, ProgressTracker progressTracker) {
        return CliqueCounting.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        ).compute();
    }

    ConductanceResult conductance(Graph graph, ConductanceParameters parameters, ProgressTracker progressTracker) {
        return new Conductance(
            graph,
            parameters.concurrency(),
            parameters.minBatchSize(),
            parameters.hasRelationshipWeightProperty(),
            parameters.communityProperty(),
            DefaultPool.INSTANCE,
            progressTracker
        ).compute();
    }

    public Labels hdbscan(Graph graph, HDBScanParameters parameters, ProgressTracker progressTracker) {
        return new HDBScan(
            graph,
            graph.nodeProperties(parameters.nodeProperty()),
            parameters,
            progressTracker,
            terminationFlag
        ).compute();
    }

    public K1ColoringResult k1Coloring(Graph graph, K1ColoringParameters parameters, ProgressTracker progressTracker) {
        var k1ColoringStub = new K1ColoringStub();
        return k1ColoringStub.k1Coloring(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );
    }
    
    KCoreDecompositionResult kCore(
        Graph graph,
        KCoreDecompositionParameters parameters,
        ProgressTracker progressTracker
    ) {
        return new KCoreDecomposition(
            graph, 
            parameters.concurrency(),
            progressTracker,
            terminationFlag
        ).compute();
    }

    public KmeansResult kMeans(Graph graph, KmeansParameters parameters, ProgressTracker progressTracker) {
        return Kmeans.createKmeans(
            graph,
            parameters,
            new KmeansContext(DefaultPool.INSTANCE, progressTracker),
            terminationFlag
        ).compute();
    }

    LabelPropagationResult labelPropagation(
        Graph graph,
        LabelPropagationParameters parameters,
        ProgressTracker progressTracker
    ) {
        return new LabelPropagation(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        ).compute();
    }

    LocalClusteringCoefficientResult lcc(
        Graph graph,
        LocalClusteringCoefficientParameters parameters,
        ProgressTracker progressTracker
    ) {
        return new LocalClusteringCoefficient(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            parameters.seedProperty(),
            progressTracker,
            terminationFlag
        ).compute();
    }

    public LeidenResult leiden(Graph graph, LeidenParameters parameters, ProgressTracker progressTracker) {
        var seedValues = Optional.ofNullable(parameters.seedProperty())
            .map(seedParameter -> CommunityCompanion.extractSeedingNodePropertyValues(graph, seedParameter))
            .orElse(null);

        return new Leiden(
            graph,
            parameters,
            seedValues,
            progressTracker,
            terminationFlag
        ).compute();
    }

    LouvainResult louvain(Graph graph, LouvainParameters parameters, ProgressTracker progressTracker) {
        return new Louvain(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        ).compute();
    }

    ModularityResult modularity(Graph graph, ModularityParameters parameters) {
        return ModularityCalculator.create(
            graph,
            graph.nodeProperties(parameters.communityProperty())::longValue,
            parameters.concurrency()
        ).compute();
    }

    ModularityOptimizationResult modularityOptimization(
        Graph graph,
        ModularityOptimizationParameters parameters,
        ProgressTracker progressTracker
    ) {
        var seedPropertyValues = parameters.seedProperty()
            .map(seedProperty -> CommunityCompanion.extractSeedingNodePropertyValues(graph, seedProperty))
            .orElse(null);

        return new ModularityOptimization(
            graph,
            parameters.maxIterations(),
            parameters.tolerance(),
            seedPropertyValues,
            parameters.concurrency(),
            parameters.batchSize(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        ).compute();
    }

    public HugeLongArray scc(Graph graph, SccParameters parameters, ProgressTracker progressTracker) {
        return new Scc(
            graph,
            progressTracker,
            terminationFlag
        ).compute();
    }

    public TriangleCountResult triangleCount(
        Graph graph,
        TriangleCountParameters parameters,
        ProgressTracker progressTracker
    ) {
        return IntersectingTriangleCount.create(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            parameters.labelFilter(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        ).compute();
    }

    Stream<TriangleResult> triangles(Graph graph, TriangleCountParameters parameters) {
        return TriangleStream.create(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.labelFilter(),
            terminationFlag
        ).compute();
    }

    public DisjointSetStruct wcc(Graph graph, WccParameters parameters, ProgressTracker progressTracker) {
        var wccStub = new WccStub(terminationFlag);
        return wccStub.wcc(graph, parameters, progressTracker);
    }

    PregelResult speakerListenerLPA(
        Graph graph,
        SpeakerListenerLPAConfig configuration,
        ProgressTracker progressTracker
    ) {
        return new SpeakerListenerLPA(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker,
            Optional.empty()
        ).compute();
    }
}
