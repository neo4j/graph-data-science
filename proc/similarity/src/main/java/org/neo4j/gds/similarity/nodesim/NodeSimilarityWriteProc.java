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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityWriteProc;
import org.neo4j.gds.similarity.SimilarityWriteResult;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.similarity.nodesim.NodeSimilarityProc.NODE_SIMILARITY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class NodeSimilarityWriteProc extends SimilarityWriteProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> {

    @Procedure(name = "gds.nodeSimilarity.write", mode = WRITE)
    @Description(NODE_SIMILARITY_DESCRIPTION)
    public Stream<SimilarityWriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.nodeSimilarity.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    public String procedureName() {
        return "NodeSimilarity";
    }

    @Override
    protected NodeSimilarityWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return NodeSimilarityWriteConfig.of(username, graphName, maybeImplicitCreate, userInput);
    }

    @Override
    protected AlgorithmFactory<NodeSimilarity, NodeSimilarityWriteConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> computationResult) {
        throw new UnsupportedOperationException("NodeSimilarity does not write node properties.");
    }

    @Override
    protected AbstractResultBuilder<SimilarityWriteResult> resultBuilder(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> computeResult) {
        throw new UnsupportedOperationException("NodeSimilarity handles result building individually.");
    }

    @Override
    protected SimilarityGraphResult similarityGraphResult(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> computationResult) {
        return computationResult.result().graphResult();
    }
}
