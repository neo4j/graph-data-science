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
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultRenderer;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingBaseConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
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
import org.neo4j.gds.wcc.WccStatsConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.WCC;

public class CommunityAlgorithmsStatsModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithmsBusinessFacade communityAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms;
    private final Synchroniser synchroniser;

    CommunityAlgorithmsStatsModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithmsBusinessFacade communityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.estimationFacade = estimationFacade;
        this.communityAlgorithms = communityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.instrumentedCommunityAlgorithms = instrumentedCommunityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT cliqueCounting(
        GraphName graphName,
        CliqueCountingBaseConfig configuration,
        StatsResultBuilder<CliqueCountingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.cliqueCounting(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }


    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringBaseConfig configuration,
        StatsResultBuilder<K1ColoringResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.k1Coloring(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionBaseConfig configuration,
        StatsResultBuilder<KCoreDecompositionResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kCoreDecomposition(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansBaseConfig configuration,
        StatsResultBuilder<KmeansResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kMeans(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationBaseConfig configuration,
        StatsResultBuilder<LabelPropagationResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.labelPropagation(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientBaseConfig configuration,
        StatsResultBuilder<LocalClusteringCoefficientResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.localClusteringCoefficient(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenBaseConfig configuration,
        StatsResultBuilder<LeidenResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.leiden(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainBaseConfig configuration,
        StatsResultBuilder<LouvainResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.louvain(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT modularity(
        GraphName graphName,
        ModularityBaseConfig configuration,
        StatsResultBuilder<ModularityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.modularity(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationBaseConfig configuration,
        StatsResultBuilder<ModularityOptimizationResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.modularityOptimization(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccCommonBaseConfig configuration,
        StatsResultBuilder<HugeLongArray, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.scc(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        StatsResultBuilder<TriangleCountResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.triangleCount(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccStatsConfig configuration,
        StatsResultBuilder<DisjointSetStruct, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            WCC,
            () -> estimationFacade.wcc(configuration),
            (graph, __) -> communityAlgorithms.wcc(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT sllpa(
        GraphName graphName,
        SpeakerListenerLPAConfig configuration,
        StatsResultBuilder<PregelResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.speakerListenerLPA(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT hdbscan(
        GraphName graphName,
        HDBScanBaseConfig configuration,
        StatsResultBuilder<Labels, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.hdbscan(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }
}
