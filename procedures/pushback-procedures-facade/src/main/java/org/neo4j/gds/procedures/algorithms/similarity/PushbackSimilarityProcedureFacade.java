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
import org.neo4j.gds.procedures.algorithms.similarity.stream.PushbackSimilarityStreamProcedureFacade;
import org.neo4j.gds.procedures.algorithms.similarity.stubs.SimilarityStubs;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackSimilarityProcedureFacade implements SimilarityProcedureFacade {

    private final PushbackSimilarityStreamProcedureFacade streamProcedureFacade;

    public PushbackSimilarityProcedureFacade(PushbackSimilarityStreamProcedureFacade streamProcedureFacade) {
        this.streamProcedureFacade = streamProcedureFacade;
    }

    @Override
    public SimilarityStubs similarityStubs() {
        return null;
    }

    @Override
    public Stream<KnnStatsResult> filteredKnnStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredKnnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityStreamResult> filteredKnnStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.filteredKnn(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> filteredKnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KnnMutateResult> filteredKnnMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredKnnMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KnnWriteResult> filteredKnnWrite(String graphNameAsString, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredKnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityStatsResult> filteredNodeSimilarityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredNodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityStreamResult> filteredNodeSimilarityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredNodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityMutateResult> filteredNodeSimilarityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredNodeSimilarityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityWriteResult> filteredNodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> filteredNodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KnnStatsResult> knnStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> knnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityStreamResult> knnStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.knn(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> knnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KnnMutateResult> knnMutate(String graphNameAsString, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> knnMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KnnWriteResult> knnWrite(String graphNameAsString, Map<String, Object> rawConfiguration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> knnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityMutateResult> nodeSimilarityMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> nodeSimilarityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityStatsResult> nodeSimilarityStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> nodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityStreamResult> nodeSimilarityStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.nodeSimilarity(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> nodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SimilarityWriteResult> nodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> nodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }
}
