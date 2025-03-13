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
import org.neo4j.gds.procedures.algorithms.community.stubs.ApproximateMaximumKCutMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.HDBScanMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.K1ColoringMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KCoreMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KMeansMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LabelPropagationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LccMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LeidenMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LouvainMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.ModularityOptimizationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.SccMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.SpeakerListenerLPAMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.TriangleCountMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.WccMutateStub;

import java.util.Map;
import java.util.stream.Stream;

public interface CommunityProcedureFacade {
    ApproximateMaximumKCutMutateStub approxMaxKCutMutateStub();

    Stream<ApproxMaxKCutMutateResult> approxMaxKCutMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> approxMaxKCutMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<ApproxMaxKCutStreamResult> approxMaxKCutStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> approxMaxKCutStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ConductanceStreamResult> conductanceStream(
        String graphName,
        Map<String, Object> configuration
    );

    K1ColoringMutateStub k1ColoringMutateStub();

    Stream<K1ColoringMutateResult> k1ColoringMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> k1ColoringMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<K1ColoringStatsResult> k1ColoringStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> k1ColoringStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<K1ColoringStreamResult> k1ColoringStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> k1ColoringStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<K1ColoringWriteResult> k1ColoringWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> k1ColoringWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    KCoreMutateStub kCoreMutateStub();

    Stream<KCoreDecompositionMutateResult> kCoreMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> kCoreMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<KCoreDecompositionStatsResult> kCoreStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> kCoreStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KCoreDecompositionStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> kCoreStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KCoreDecompositionWriteResult> kCoreWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> kCoreWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    KMeansMutateStub kMeansMutateStub();

    Stream<KMeansMutateResult> kMeansMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> kMeansMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<KmeansStatsResult> kmeansStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> kmeansStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KMeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> kmeansStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KMeansWriteResult> kmeansWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> kmeansWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    LabelPropagationMutateStub labelPropagationMutateStub();

    Stream<LabelPropagationMutateResult> labelPropagationMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> labelPropagationMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<LabelPropagationStatsResult> labelPropagationStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> labelPropagationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName, Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> labelPropagationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LabelPropagationWriteResult> labelPropagationWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> labelPropagationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    LccMutateStub lccMutateStub();

    Stream<LocalClusteringCoefficientMutateResult> localClusteringCoefficientMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> localClusteringCoefficientMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<LocalClusteringCoefficientStatsResult> localClusteringCoefficientStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> localClusteringCoefficientStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LocalClusteringCoefficientStreamResult> localClusteringCoefficientStream(
        String graphName, Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> localClusteringCoefficientStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LocalClusteringCoefficientWriteResult> localClusteringCoefficientWrite(
        String graphName, Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> localClusteringCoefficientWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    LeidenMutateStub leidenMutateStub();

    Stream<LeidenMutateResult> leidenMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> leidenMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<LeidenStatsResult> leidenStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> leidenStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LeidenStreamResult> leidenStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> leidenStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LeidenWriteResult> leidenWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> leidenWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    LouvainMutateStub louvainMutateStub();

    Stream<LouvainMutateResult> louvainMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> louvainMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<LouvainStatsResult> louvainStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> louvainStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LouvainStreamResult> louvainStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> louvainStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<LouvainWriteResult> louvainWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> louvainWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ModularityStatsResult> modularityStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> modularityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ModularityStreamResult> modularityStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> modularityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    ModularityOptimizationMutateStub modularityOptimizationMutateStub();

    Stream<ModularityOptimizationMutateResult> modularityOptimizationMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> modularityOptimizationMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<ModularityOptimizationStatsResult> modularityOptimizationStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> modularityOptimizationStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ModularityOptimizationStreamResult> modularityOptimizationStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> modularityOptimizationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ModularityOptimizationWriteResult> modularityOptimizationWrite(
        String graphName, Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> modularityOptimizationWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    SccMutateStub sccMutateStub();

    Stream<SccMutateResult> sccMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> sccMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<SccStatsResult> sccStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> sccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SccStreamResult> sccStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> sccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SccWriteResult> sccWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<AlphaSccWriteResult> sccWriteAlpha(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> sccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    TriangleCountMutateStub triangleCountMutateStub();

    Stream<TriangleCountMutateResult> triangleCountMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> triangleCountMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<TriangleCountStatsResult> triangleCountStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> triangleCountStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<TriangleCountStreamResult> triangleCountStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> triangleCountStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<TriangleCountWriteResult> triangleCountWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> triangleCountWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<TriangleStreamResult> trianglesStream(String graphName, Map<String, Object> configuration);

    WccMutateStub wccMutateStub();

    Stream<WccMutateResult> wccMutate(
        String graphName,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> wccMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<WccStatsResult> wccStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> wccStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<WccStreamResult> wccStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> wccStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<WccWriteResult> wccWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> wccWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<MemoryEstimateResult> sllpaStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SpeakerListenerLPAStreamResult> sllpaStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> sllpaStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SpeakerListenerLPAStatsResult> sllpaStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> sllpaMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SpeakerListenerLPAMutateResult> sllpaMutate(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> sllpaWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SpeakerListenerLPAWriteResult> sllpaWrite(
        String graphName,
        Map<String, Object> configuration
    );


    SpeakerListenerLPAMutateStub speakerListenerLPAMutateStub();

    HDBScanMutateStub hdbscanMutateStub();

    Stream<HDBScanMutateResult> hdbscanMutate(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> hdbscanMutateEstimate(Object graphName, Map<String, Object> configuration);

    Stream<HDBScanStatsResult> hdbscanStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> hdbscanStatsEstimate(Object graphName, Map<String, Object> configuration);

    Stream<HDBScanStreamResult> hdbscanStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> hdbscanStreamEstimate(Object graphName, Map<String, Object> configuration);

    Stream<HDBScanWriteResult> hdbscanWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> hdbscanWriteEstimate(Object graphName, Map<String, Object> configuration);

}
