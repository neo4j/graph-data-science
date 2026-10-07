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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultRenderer;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingBaseConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
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
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.LCC;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.LabelPropagation;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Leiden;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Louvain;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ModularityOptimization;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SCC;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.WCC;

public class CommunityAlgorithmsStreamModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithmsBusinessFacade algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms;
    private final Synchroniser synchroniser;

    CommunityAlgorithmsStreamModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithmsBusinessFacade algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.instrumentedCommunityAlgorithms = instrumentedCommunityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> Stream<RESULT> approximateMaximumKCut(
        GraphName graphName,
        ApproxMaxKCutBaseConfig configuration,
        StreamResultBuilder<ApproxMaxKCutResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.approximateMaximumKCut(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> cliqueCounting(
        GraphName graphName,
        CliqueCountingBaseConfig configuration,
        StreamResultBuilder<CliqueCountingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.cliqueCounting(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> conductance(
        GraphName graphName,
        ConductanceBaseConfig configuration,
        StreamResultBuilder<ConductanceResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.conductance(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> hdbscan(
        GraphName graphName,
        HDBScanBaseConfig configuration,
        StreamResultBuilder<Labels, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.hdbscan(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> k1Coloring(
        GraphName graphName,
        K1ColoringBaseConfig configuration,
        StreamResultBuilder<K1ColoringResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.k1Coloring(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> kCore(
        GraphName graphName,
        KCoreDecompositionBaseConfig configuration,
        StreamResultBuilder<KCoreDecompositionResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kCoreDecomposition(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> kMeans(
        GraphName graphName,
        KmeansBaseConfig configuration,
        StreamResultBuilder<KmeansResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kMeans(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> labelPropagation(
        GraphName graphName,
        LabelPropagationStreamConfig configuration,
        StreamResultBuilder<LabelPropagationResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LabelPropagation,
            estimationFacade::labelPropagation,
            (graph, __) -> algorithms.labelPropagation(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> lcc(
        GraphName graphName,
        LocalClusteringCoefficientStreamConfig configuration,
        StreamResultBuilder<LocalClusteringCoefficientResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LCC,
            () -> estimationFacade.lcc(configuration),
            (graph, __) -> algorithms.lcc(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> leiden(
        GraphName graphName,
        LeidenStreamConfig configuration,
        StreamResultBuilder<LeidenResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Leiden,
            () -> estimationFacade.leiden(configuration),
            (graph, __) -> algorithms.leiden(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> louvain(
        GraphName graphName,
        LouvainStreamConfig configuration,
        StreamResultBuilder<LouvainResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Louvain,
            () -> estimationFacade.louvain(configuration),
            (graph, __) -> algorithms.louvain(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> modularity(
        GraphName graphName,
        ModularityBaseConfig configuration,
        StreamResultBuilder<ModularityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.modularity(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> modularityOptimization(
        GraphName graphName,
        ModularityOptimizationStreamConfig configuration,
        StreamResultBuilder<ModularityOptimizationResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ModularityOptimization,
            estimationFacade::modularityOptimization,
            (graph, __) -> algorithms.modularityOptimization(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> scc(
        GraphName graphName,
        SccStreamConfig configuration,
        StreamResultBuilder<HugeLongArray, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            SCC,
            estimationFacade::scc,
            (graph, __) -> algorithms.scc(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> triangleCount(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        StreamResultBuilder<TriangleCountResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.triangleCount(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> triangles(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        StreamResultBuilder<Stream<TriangleResult>, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.triangles(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> wcc(
        GraphName graphName,
        WccStreamConfig configuration,
        StreamResultBuilder<DisjointSetStruct, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            WCC,
            () -> estimationFacade.wcc(configuration),
            (graph, __) -> algorithms.wcc(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> sllpa(
        GraphName graphName,
        SpeakerListenerLPAConfig configuration,
        StreamResultBuilder<PregelResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.speakerListenerLPA(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }
}
