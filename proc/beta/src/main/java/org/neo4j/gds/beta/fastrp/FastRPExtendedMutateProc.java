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
package org.neo4j.gds.beta.fastrp;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.fastrp.FastRPExtendedCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class FastRPExtendedMutateProc extends MutatePropertyProc<FastRP, FastRP.FastRPResult, FastRPExtendedMutateProc.MutateResult, FastRPExtendedMutateConfig> {

    @Procedure(value = "gds.beta.fastRPExtended.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPExtendedMutateConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return mutate(computationResult);
    }

    @Procedure(value = "gds.beta.fastRPExtended.mutate.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<FastRP, FastRP.FastRPResult, FastRPExtendedMutateConfig> computationResult) {
        return FastRPExtendedCompanion.getNodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<FastRP, FastRP.FastRPResult, FastRPExtendedMutateConfig> computeResult) {
        return new MutateResult.Builder();
    }

    @Override
    protected FastRPExtendedMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return FastRPExtendedMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<FastRP, FastRPExtendedMutateConfig> algorithmFactory() {
        return new FastRPExtendedFactory<>();
    }

    @SuppressWarnings("unused")
    public static final class MutateResult {

        public final long nodePropertiesWritten;
        public final long mutateMillis;
        public final long nodeCount;
        public final long createMillis;
        public final long computeMillis;
        public final Map<String, Object> configuration;

        MutateResult(
            long nodeCount,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long mutateMillis,
            Map<String, Object> config
        ) {
            this.nodeCount = nodeCount;
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.configuration = config;
        }

        static final class Builder extends AbstractResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
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
