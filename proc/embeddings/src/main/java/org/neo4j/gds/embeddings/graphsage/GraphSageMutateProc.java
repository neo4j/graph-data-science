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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPHSAGE_DESCRIPTION;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.getActualConfig;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.getNodeProperties;
import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.injectRelationshipWeightPropertyFromModel;
import static org.neo4j.procedure.Mode.READ;

public class GraphSageMutateProc extends MutatePropertyProc<GraphSage, GraphSage.GraphSageResult, GraphSageMutateProc.MutateResult, GraphSageMutateConfig> {

    @Context
    public ModelCatalog modelCatalog;

    @Procedure(value = "gds.beta.graphSage.mutate", mode = Mode.READ)
    @Description(GRAPHSAGE_DESCRIPTION)
    public Stream<GraphSageMutateProc.MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        injectRelationshipWeightPropertyFromModel(getActualConfig(graphNameOrConfig, configuration), modelCatalog, username.username());

        ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageMutateConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return mutate(computationResult);
    }

    @Procedure(value = "gds.beta.graphSage.mutate.estimate", mode = READ)
    @Description(GRAPHSAGE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        injectRelationshipWeightPropertyFromModel(getActualConfig(graphNameOrConfig, configuration), modelCatalog, username.username());

        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageMutateConfig> computationResult) {
        return getNodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<GraphSageMutateProc.MutateResult> resultBuilder(ComputationResult<GraphSage, GraphSage.GraphSageResult, GraphSageMutateConfig> computeResult) {
        return new MutateResult.Builder();
    }

    @Override
    public ValidationConfiguration<GraphSageMutateConfig> getValidationConfig() {
        return GraphSageCompanion.getValidationConfig(modelCatalog, username());
    }

    @Override
    protected GraphSageMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return GraphSageMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<GraphSage, GraphSageMutateConfig> algorithmFactory() {
        return new GraphSageAlgorithmFactory<>(modelCatalog);
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
