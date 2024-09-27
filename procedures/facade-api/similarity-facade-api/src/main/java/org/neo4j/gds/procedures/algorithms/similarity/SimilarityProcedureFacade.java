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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.similarity.stubs.FilteredKnnMutateStub;
import org.neo4j.gds.procedures.algorithms.similarity.stubs.FilteredNodeSimilarityMutateStub;
import org.neo4j.gds.procedures.algorithms.similarity.stubs.KnnMutateStub;
import org.neo4j.gds.procedures.algorithms.similarity.stubs.NodeSimilarityMutateStub;

import java.util.Map;
import java.util.stream.Stream;

public interface SimilarityProcedureFacade {
    FilteredKnnMutateStub filteredKnnMutateStub();

    Stream<KnnStatsResult> filteredKnnStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> filteredKnnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SimilarityStreamResult> filteredKnnStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> filteredKnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KnnWriteResult> filteredKnnWrite(String graphNameAsString, Map<String, Object> rawConfiguration);

    Stream<MemoryEstimateResult> filteredKnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    FilteredNodeSimilarityMutateStub filteredNodeSimilarityMutateStub();

    Stream<SimilarityStatsResult> filteredNodeSimilarityStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> filteredNodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SimilarityStreamResult> filteredNodeSimilarityStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> filteredNodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SimilarityWriteResult> filteredNodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> filteredNodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    KnnMutateStub knnMutateStub();

    Stream<KnnStatsResult> knnStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> knnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SimilarityStreamResult> knnStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> knnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KnnWriteResult> knnWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> knnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    NodeSimilarityMutateStub nodeSimilarityMutateStub();

    Stream<SimilarityStatsResult> nodeSimilarityStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> nodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SimilarityStreamResult> nodeSimilarityStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> nodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SimilarityWriteResult> nodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    );

    Stream<MemoryEstimateResult> nodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );
}
