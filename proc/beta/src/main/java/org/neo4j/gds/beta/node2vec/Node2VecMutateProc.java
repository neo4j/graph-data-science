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
package org.neo4j.gds.beta.node2vec;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecAlgorithmFactory;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.ml.core.tensor.FloatVector;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.MutatePropertyProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class Node2VecMutateProc extends MutatePropertyProc<Node2Vec, HugeObjectArray<FloatVector>, Node2VecMutateProc.MutateResult, Node2VecMutateConfig> {

    @Procedure(value = "gds.beta.node2vec.mutate", mode = READ)
    @Description(Node2VecCompanion.DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Node2Vec, HugeObjectArray<FloatVector>, Node2VecMutateConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        return mutate(computationResult);
    }

    @Procedure(value = "gds.beta.node2vec.mutate.estimate", mode = READ)
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
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
    protected AlgorithmFactory<Node2Vec, Node2VecMutateConfig> algorithmFactory() {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<Node2Vec, HugeObjectArray<FloatVector>, Node2VecMutateConfig> computationResult) {
        return Node2VecCompanion.nodeProperties(computationResult);
    }

    @Override
    protected MutateResult.Builder resultBuilder(ComputationResult<Node2Vec, HugeObjectArray<FloatVector>, Node2VecMutateConfig> computeResult) {
        return new MutateResult.Builder();
    }

    public static final class MutateResult extends StandardMutateResult {

        public final long nodeCount;
        public final long nodePropertiesWritten;

        MutateResult(
            long nodeCount,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long mutateMillis,
            Map<String, Object> configuration
        ) {
            super(createMillis, computeMillis, 0, mutateMillis, configuration);
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    nodeCount,
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    writeMillis,
                    config.toMap()
                );
            }
        }
    }
}
