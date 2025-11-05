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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.community.stats.PushbackCommunityStatsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.community.stream.PushbackCommunityStreamProcedureFacade;
import org.neo4j.gds.procedures.algorithms.community.stubs.CommunityStubs;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCommunityProcedureFacade implements  CommunityProcedureFacade{

    private final PushbackCommunityStreamProcedureFacade streamProcedureFacade;
    private final PushbackCommunityStatsProcedureFacade statsProcedureFacade;

    public PushbackCommunityProcedureFacade(PushbackCommunityStreamProcedureFacade streamProcedureFacade,
        PushbackCommunityStatsProcedureFacade statsProcedureFacade
    ) {
        this.streamProcedureFacade = streamProcedureFacade;
        this.statsProcedureFacade = statsProcedureFacade;
    }

    @Override
    public CommunityStubs communityStubs() {
        return null;
    }

    @Override
    public Stream<ApproxMaxKCutMutateResult> approxMaxKCutMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> approxMaxKCutMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ApproxMaxKCutStreamResult> approxMaxKCutStream(String graphName, Map<String, Object> configuration) {
        return  streamProcedureFacade.approxMaxKCut(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> approxMaxKCutStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ApproxMaxKCutWriteResult> approxMaxKCutWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<CliqueCountingMutateResult> cliqueCountingMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> cliqueCountingMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CliqueCountingStatsResult> cliqueCountingStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.cliqueCounting(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> cliqueCountingStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CliqueCountingStreamResult> cliqueCountingStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.cliqueCounting(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> cliqueCountingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CliqueCountingWriteResult> cliqueCountingWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> cliqueCountingWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ConductanceStreamResult> conductanceStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.conductance(graphName,configuration);
    }

    @Override
    public Stream<K1ColoringMutateResult> k1ColoringMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> k1ColoringMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<K1ColoringStatsResult> k1ColoringStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.k1Coloring(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> k1ColoringStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<K1ColoringStreamResult> k1ColoringStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.k1Coloring(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> k1ColoringStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<K1ColoringWriteResult> k1ColoringWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> k1ColoringWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KCoreDecompositionMutateResult> kCoreMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> kCoreMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KCoreDecompositionStatsResult> kCoreStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.kCore(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> kCoreStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KCoreDecompositionStreamResult> kCoreStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.kCore(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> kCoreStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KCoreDecompositionWriteResult> kCoreWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> kCoreWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KMeansMutateResult> kMeansMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> kMeansMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KmeansStatsResult> kmeansStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.kMeans(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> kmeansStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KMeansStreamResult> kmeansStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.kMeans(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> kmeansStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KMeansWriteResult> kmeansWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> kmeansWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LabelPropagationMutateResult> labelPropagationMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> labelPropagationMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LabelPropagationStatsResult> labelPropagationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.labelPropagation(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> labelPropagationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.labelPropagation(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> labelPropagationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LabelPropagationWriteResult> labelPropagationWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> labelPropagationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LocalClusteringCoefficientMutateResult> localClusteringCoefficientMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> localClusteringCoefficientMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LocalClusteringCoefficientStatsResult> localClusteringCoefficientStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.lcc(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> localClusteringCoefficientStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LocalClusteringCoefficientStreamResult> localClusteringCoefficientStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.lcc(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> localClusteringCoefficientStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LocalClusteringCoefficientWriteResult> localClusteringCoefficientWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> localClusteringCoefficientWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LeidenMutateResult> leidenMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> leidenMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LeidenStatsResult> leidenStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.leiden(graphName, configuration);

    }

    @Override
    public Stream<MemoryEstimateResult> leidenStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LeidenStreamResult> leidenStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.leiden(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> leidenStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LeidenWriteResult> leidenWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> leidenWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LouvainMutateResult> louvainMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> louvainMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LouvainStatsResult> louvainStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.louvain(graphName, configuration);

    }

    @Override
    public Stream<MemoryEstimateResult> louvainStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LouvainStreamResult> louvainStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.louvain(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> louvainStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<LouvainWriteResult> louvainWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> louvainWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ModularityStatsResult> modularityStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.modularity(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> modularityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ModularityStreamResult> modularityStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.modularity(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> modularityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ModularityOptimizationMutateResult> modularityOptimizationMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> modularityOptimizationMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ModularityOptimizationStatsResult> modularityOptimizationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.modularityOptimization(graphName, configuration);

    }

    @Override
    public Stream<MemoryEstimateResult> modularityOptimizationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ModularityOptimizationStreamResult> modularityOptimizationStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.modularityOptimization(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> modularityOptimizationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ModularityOptimizationWriteResult> modularityOptimizationWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> modularityOptimizationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SccMutateResult> sccMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> sccMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SccStatsResult> sccStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.scc(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> sccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SccStreamResult> sccStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.scc(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> sccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SccWriteResult> sccWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<AlphaSccWriteResult> sccWriteAlpha(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> sccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TriangleCountMutateResult> triangleCountMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> triangleCountMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TriangleCountStatsResult> triangleCountStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.triangleCount(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> triangleCountStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TriangleCountStreamResult> triangleCountStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.triangleCount(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> triangleCountStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TriangleCountWriteResult> triangleCountWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> triangleCountWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TriangleStreamResult> trianglesStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.triangles(graphName,configuration);
    }

    @Override
    public Stream<WccMutateResult> wccMutate(String graphName, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> wccMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<WccStatsResult> wccStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.wcc(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> wccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<WccStreamResult> wccStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.wcc(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> wccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<WccWriteResult> wccWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> wccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> sllpaStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpeakerListenerLPAStreamResult> sllpaStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.sllpa(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> sllpaStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpeakerListenerLPAStatsResult> sllpaStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> sllpaMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpeakerListenerLPAMutateResult> sllpaMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> sllpaWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpeakerListenerLPAWriteResult> sllpaWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<HDBScanMutateResult> hdbscanMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hdbscanMutateEstimate(Object graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<HDBScanStatsResult> hdbscanStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.hdbscan(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> hdbscanStatsEstimate(Object graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<HDBScanStreamResult> hdbscanStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.hdbscan(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> hdbscanStreamEstimate(Object graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<HDBScanWriteResult> hdbscanWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hdbscanWriteEstimate(Object graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }
}
