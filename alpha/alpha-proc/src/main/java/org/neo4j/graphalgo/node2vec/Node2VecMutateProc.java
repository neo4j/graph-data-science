/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.node2vec;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.node2vec.Node2Vec;
import org.neo4j.graphalgo.impl.node2vec.Node2VecMutateConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.node2vec.Node2VecStreamProc.NODE2VEC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class Node2VecMutateProc extends MutateProc<Node2Vec, Node2Vec, Node2VecMutateProc.MutateResult, Node2VecMutateConfig> {

    @Procedure(value = "gds.alpha.node2vec.mutate", mode = READ)
    @Description(NODE2VEC_DESCRIPTION)
    public Stream<Node2VecMutateProc.MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    )  {
        ComputationResult<Node2Vec, Node2Vec, Node2VecMutateConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return mutate(computationResult);
    }

    @Override
    protected Node2VecMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return Node2VecMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Node2Vec, Node2VecMutateConfig> algorithmFactory(Node2VecMutateConfig config) {
        return new AlphaAlgorithmFactory<>() {
            @Override
            public Node2Vec buildAlphaAlgo(
                Graph graph, Node2VecMutateConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new Node2Vec(graph, config);
            }
        };
    }

    @Override
    protected PropertyTranslator<Node2Vec> nodePropertyTranslator(ComputationResult<Node2Vec, Node2Vec, Node2VecMutateConfig> computationResult) {
        return (PropertyTranslator.OfDoubleArray<Node2Vec>) Node2Vec::embeddingForNode;
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<Node2Vec, Node2Vec, Node2VecMutateConfig> computeResult) {
        return new MutateResult.Builder();
    }

    public static class MutateResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final Map<String, Object> configuration;

        public MutateResult(
            long nodeCount,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long mutateMillis,
            Map<String, Object> configuration
        ) {

            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.configuration = configuration;
        }

        static class Builder extends AbstractResultBuilder<Node2VecMutateProc.MutateResult> {

            @Override
            public Node2VecMutateProc.MutateResult build() {
                return new Node2VecMutateProc.MutateResult(
                    nodeCount,
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    config.toMap()
                );
            }
        }
    }
}
