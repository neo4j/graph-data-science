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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingBaseConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreFilteredETL;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAllLabels;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAnyLabel;
import org.neo4j.gds.core.loading.validation.OptionalNodePropertyGraphStoreRequirement;
import org.neo4j.gds.core.loading.validation.PregelPropertiesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
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
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Community algorithms in all modes need to go through here, lest we have duplication.
 */
public final class InstrumentedCommunityAlgorithms {
    private final TrackedCommunityAlgorithms algorithms;
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    private InstrumentedCommunityAlgorithms(
        TrackedCommunityAlgorithms algorithms,
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public static InstrumentedCommunityAlgorithms create(
        Log log,
        ProgressTrackerCreator progressTrackerCreator,
        TerminationFlag terminationFlag,
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        var algorithms = new CommunityAlgorithms(log, terminationFlag);
        var trackedCommunityAlgorithms = new TrackedCommunityAlgorithms(log, progressTrackerCreator, algorithms);

        return new InstrumentedCommunityAlgorithms(trackedCommunityAlgorithms, estimationFacade, launchConvenience);
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> approximateMaximumKCut(
        GraphName graphName,
        ApproxMaxKCutBaseConfig configuration,
        Optional<SideEffect<ApproxMaxKCutResult, METADATA>> sideEffect,
        ResultRenderer<ApproxMaxKCutResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new MinCommunitySizeSumETL(configuration.minCommunitySizes()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.approximateMaximumKCut(graph, configuration),
            () -> estimationFacade.approximateMaximumKCut(configuration),
            AlgorithmLabel.ApproximateMaximumKCut,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> cliqueCounting(
        GraphName graphName,
        CliqueCountingBaseConfig configuration,
        Optional<SideEffect<CliqueCountingResult, METADATA>> sideEffect,
        ResultRenderer<CliqueCountingResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(
                (graphStore, selectedLabels, selectedRelationshipTypes, relationshipProperty) ->
                    new UndirectedOnlyRequirement("Clique Counting").validate(
                        graphStore,
                        selectedLabels,
                        selectedRelationshipTypes
                    )
            );

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.cliqueCounting(graph, configuration),
            estimationFacade::cliqueCounting,
            AlgorithmLabel.CliqueCounting,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> conductance(
        GraphName graphName,
        ConductanceBaseConfig configuration,
        Optional<SideEffect<ConductanceResult, METADATA>> sideEffect,
        ResultRenderer<ConductanceResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new NodePropertyMustExistOnAnyLabel(configuration.communityProperty()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.conductance(graph, configuration),
            estimationFacade::conductance,
            AlgorithmLabel.Conductance,
            sideEffect,
            resultRenderer
        );
    }

    /**
     * Interestingly, in de-duplicating, we find that the old code had no special validation,
     * but the duplicated code had added a `new NodePropertyMustExistOnAnyLabel("nodeProperty")`.
     * Tests break when applying that validation. The duplicated code had no tests, and was never integrated.
     * There is a possibility that this validation should be there, can't rule it out.
     * If so, tests need to change, and hopefully it would not break customers' code.
     * For now though it will live in Javadoc...
     */
    public <RESULT, METADATA> CompletableFuture<RESULT> hdbscan(
        GraphName graphName,
        HDBScanBaseConfig configuration,
        Optional<SideEffect<Labels, METADATA>> sideEffect,
        ResultRenderer<Labels, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.hdbscan(graph, configuration),
            () -> estimationFacade.hdbscan(configuration),
            AlgorithmLabel.HDBScan,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> k1Coloring(
        GraphName graphName,
        K1ColoringBaseConfig configuration,
        Optional<SideEffect<K1ColoringResult, METADATA>> sideEffect,
        ResultRenderer<K1ColoringResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.k1Coloring(graph, configuration),
            estimationFacade::k1Coloring,
            AlgorithmLabel.K1Coloring,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> kCoreDecomposition(
        GraphName graphName,
        KCoreDecompositionBaseConfig configuration,
        Optional<SideEffect<KCoreDecompositionResult, METADATA>> sideEffect,
        ResultRenderer<KCoreDecompositionResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new UndirectedOnlyRequirement("K-Core-Decomposition").validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.kCoreDecomposition(graph, configuration),
            estimationFacade::kCoreDecomposition,
            AlgorithmLabel.KCoreDecomposition,
            sideEffect,
            resultRenderer
        );
    }

    /**
     * In the duplicated code, someone had inserted a `new NodePropertyTypeRequirement("nodeProperty",List.of(ValueType.DOUBLE_ARRAY, ValueType.FLOAT_ARRAY, ValueType.DOUBLE))` clause.
     * When applying it here, it breaks some tests. So I have left it out.
     * The duplicated code after all was never run, so seems illegitimate.
     */
    public <RESULT, METADATA> CompletableFuture<RESULT> kMeans(
        GraphName graphName,
        KmeansBaseConfig configuration,
        Optional<SideEffect<KmeansResult, METADATA>> sideEffect,
        ResultRenderer<KmeansResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.kMeans(graph, configuration),
            () -> estimationFacade.kMeans(configuration),
            AlgorithmLabel.KMeans,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> labelPropagation(
        GraphName graphName,
        LabelPropagationBaseConfig configuration,
        Optional<SideEffect<LabelPropagationResult, METADATA>> sideEffect,
        ResultRenderer<LabelPropagationResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(OptionalNodePropertyGraphStoreRequirement.create(Optional.ofNullable(configuration.seedProperty())))
            .with(new NodePropertyMustExistOnAllLabels(configuration.nodeWeightProperty()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.labelPropagation(graph, configuration),
            estimationFacade::labelPropagation,
            AlgorithmLabel.LabelPropagation,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> localClusteringCoefficient(
        GraphName graphName,
        LocalClusteringCoefficientBaseConfig configuration,
        Optional<SideEffect<LocalClusteringCoefficientResult, METADATA>> sideEffect,
        ResultRenderer<LocalClusteringCoefficientResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(OptionalNodePropertyGraphStoreRequirement.create(Optional.ofNullable(configuration.seedProperty())))
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new UndirectedOnlyRequirement("LocalClusteringCoefficient").validate(
                        graphStore,
                        selectedLabels,
                        selectedRelationshipTypes
                    );
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.localClusteringCoefficient(graph, configuration),
            () -> estimationFacade.localClusteringCoefficient(configuration),
            AlgorithmLabel.LocalClusteringCoefficient,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> leiden(
        GraphName graphName,
        LeidenBaseConfig configuration,
        Optional<SideEffect<LeidenResult, METADATA>> sideEffect,
        ResultRenderer<LeidenResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(OptionalNodePropertyGraphStoreRequirement.create(Optional.ofNullable(configuration.seedProperty())))
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new UndirectedOnlyRequirement("Leiden").validate(
                        graphStore,
                        selectedLabels,
                        selectedRelationshipTypes
                    );
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.leiden(graph, configuration),
            () -> estimationFacade.leiden(configuration),
            AlgorithmLabel.Leiden,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> louvain(
        GraphName graphName,
        LouvainBaseConfig configuration,
        Optional<SideEffect<LouvainResult, METADATA>> sideEffect,
        ResultRenderer<LouvainResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(OptionalNodePropertyGraphStoreRequirement.create(Optional.ofNullable(configuration.seedProperty())));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.louvain(graph, configuration),
            () -> estimationFacade.louvain(configuration),
            AlgorithmLabel.Louvain,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> modularity(
        GraphName graphName,
        ModularityBaseConfig configuration,
        Optional<SideEffect<ModularityResult, METADATA>> sideEffect,
        ResultRenderer<ModularityResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new NodePropertyMustExistOnAnyLabel(configuration.communityProperty()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.modularity(graph, configuration),
            estimationFacade::modularity,
            AlgorithmLabel.Modularity,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> modularityOptimization(
        GraphName graphName,
        ModularityOptimizationBaseConfig configuration,
        Optional<SideEffect<ModularityOptimizationResult, METADATA>> sideEffect,
        ResultRenderer<ModularityOptimizationResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(OptionalNodePropertyGraphStoreRequirement.create(Optional.ofNullable(configuration.seedProperty())));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.modularityOptimization(graph, configuration),
            estimationFacade::modularityOptimization,
            AlgorithmLabel.ModularityOptimization,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> scc(
        GraphName graphName,
        SccCommonBaseConfig configuration,
        Optional<SideEffect<HugeLongArray, METADATA>> sideEffect,
        ResultRenderer<HugeLongArray, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.scc(graph, configuration),
            estimationFacade::scc,
            AlgorithmLabel.SCC,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> speakerListenerLPA(
        GraphName graphName,
        SpeakerListenerLPAConfig configuration,
        Optional<SideEffect<PregelResult, METADATA>> sideEffect,
        ResultRenderer<PregelResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new PregelPropertiesRequirement(configuration.writeProperty(), configuration.writeToResultStore()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.speakerListenerLPA(graph, configuration),
            estimationFacade::speakerListenerLPA,
            AlgorithmLabel.SLLPA,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> triangles(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        Optional<SideEffect<Stream<TriangleResult>, METADATA>> sideEffect,
        ResultRenderer<Stream<TriangleResult>, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(TriangleCountGraphStoreRequirements.create(configuration.labelFilter()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.triangles(graph, configuration),
            estimationFacade::triangles,
            AlgorithmLabel.Triangles,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> triangleCount(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        Optional<SideEffect<TriangleCountResult, METADATA>> sideEffect,
        ResultRenderer<TriangleCountResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(TriangleCountGraphStoreRequirements.create(configuration.labelFilter()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.triangleCount(graph, configuration),
            estimationFacade::triangleCount,
            AlgorithmLabel.TriangleCount,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> wcc(
        GraphName graphName,
        WccBaseConfig configuration,
        Optional<SideEffect<DisjointSetStruct, METADATA>> sideEffect,
        ResultRenderer<DisjointSetStruct, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(OptionalNodePropertyGraphStoreRequirement.create(Optional.ofNullable(configuration.seedProperty())));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.wcc(graph, configuration),
            () -> estimationFacade.wcc(configuration),
            AlgorithmLabel.WCC,
            sideEffect,
            resultRenderer
        );
    }
}
