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
package org.neo4j.gds.procedures.algorithms.embeddings;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.embeddings.stats.PushbackNodeEmbeddingsStatsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.embeddings.stream.PushbackNodeEmbeddingsStreamProcedureFacade;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.NodeEmbeddingsStubs;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackNodeEmbeddingsProcedureFacade implements NodeEmbeddingsProcedureFacade {

    private final PushbackNodeEmbeddingsStreamProcedureFacade streamProcedureFacade;
    private final PushbackNodeEmbeddingsStatsProcedureFacade statsProcedureFacade;

    public PushbackNodeEmbeddingsProcedureFacade(
        PushbackNodeEmbeddingsStreamProcedureFacade streamProcedureFacade,
        PushbackNodeEmbeddingsStatsProcedureFacade statsProcedureFacade
    ) {
        this.streamProcedureFacade = streamProcedureFacade;
        this.statsProcedureFacade = statsProcedureFacade;
    }

    @Override
    public NodeEmbeddingsStubs nodeEmbeddingStubs() {
        return null;
    }

    @Override
    public Stream<FastRPStatsResult> fastRPStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.fastRP(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> fastRPStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> fastRPStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.fastRP(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> fastRPStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> fastRPMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> fastRPMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingsWriteResult> fastRPWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> fastRPWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> graphSageStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<MemoryEstimateResult> graphSageStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> graphSageMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<MemoryEstimateResult> graphSageMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<GraphSageTrainResult> graphSageTrain(String graphName, Map<String, Object> configuration) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<MemoryEstimateResult> graphSageTrainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<DefaultNodeEmbeddingsWriteResult> graphSageWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<MemoryEstimateResult> graphSageWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        throw new UnsupportedOperationException("GraphSage is currently not supported.");
    }

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> hashGnnStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.hashGnn(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> hashGnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> hashGnnMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hashGnnMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingsWriteResult> hashGnnWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hashGnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> node2VecStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.node2Vec(graphName, configuration);

    }

    @Override
    public Stream<MemoryEstimateResult> node2VecStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<Node2VecMutateResult> node2VecMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> node2VecMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<Node2VecWriteResult> node2VecWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> node2VecWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }
}
