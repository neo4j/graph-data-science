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
import org.neo4j.gds.procedures.algorithms.community.stubs.K1ColoringMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KCoreMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.KMeansMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LabelPropagationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LccMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LeidenMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.LouvainMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.ModularityOptimizationMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.SccMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.TriangleCountMutateStub;
import org.neo4j.gds.procedures.algorithms.community.stubs.WccMutateStub;

import java.util.Map;
import java.util.stream.Stream;

public interface CommunityProcedureFacade {
    ApproximateMaximumKCutMutateStub approximateMaximumKCutMutateStub();

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

    Stream<KmeansStatsResult> kmeansStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> kmeansStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KmeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> kmeansStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KmeansWriteResult> kmeansWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> kmeansWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    LabelPropagationMutateStub labelPropagationMutateStub();

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
}
