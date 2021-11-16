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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutateProc;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.gds.validation.ValidationConfig;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.NumberType;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.linkmodels.LinkPredictionPredictCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LinkPredictionPredictMutateProc extends MutateProc<LinkPredictionPredict, ExhaustiveLinkPredictionResult, LinkPredictionPredictMutateProc.MutateResult, LinkPredictionPredictMutateConfig> {

    @Context
    public ModelCatalog modelCatalog;

    @Procedure(name = "gds.alpha.ml.linkPrediction.predict.mutate", mode = Mode.READ)
    @Description(DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(name = "gds.alpha.ml.linkPrediction.predict.mutate.estimate", mode = READ)
    @Description("Estimates memory for applying a linkPrediction model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    public ValidationConfig<LinkPredictionPredictMutateConfig> getValidationConfig() {
        return LinkPredictionPredictCompanion.getValidationConfig();
    }

    @Override
    protected LinkPredictionPredictMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LinkPredictionPredictMutateConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<LinkPredictionPredict, LinkPredictionPredictMutateConfig> algorithmFactory() {
        return new LinkPredictionPredictFactory<>(modelCatalog);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<LinkPredictionPredict, ExhaustiveLinkPredictionResult, LinkPredictionPredictMutateConfig> computeResult
    ) {
        return new MutateResult.Builder();
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<LinkPredictionPredict, ExhaustiveLinkPredictionResult, LinkPredictionPredictMutateConfig> computationResult
    ) {
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(computationResult.graph())
            .orientation(Orientation.UNDIRECTED)
            .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .allocationTracker(allocationTracker())
            .build();

        computationResult
            .result()
            .stream()
            .forEach(predictedLink -> relationshipsBuilder.addFromInternal(predictedLink.sourceId(),
                predictedLink.targetId(),
                predictedLink.probability()
            ));
        var relationships = relationshipsBuilder.build();

        var config = computationResult.config();
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            computationResult.graphStore().addRelationshipType(
                RelationshipType.of(config.mutateRelationshipType()),
                Optional.of(config.mutateProperty()),
                Optional.of(NumberType.FLOATING_POINT),
                relationships
            );
        }
        resultBuilder.withRelationshipsWritten(relationships.topology().elementCount());
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends StandardMutateResult {

        public final long relationshipsWritten;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long relationshipsWritten,
            Map<String, Object> configuration
        ) {
            super(
                createMillis,
                computeMillis,
                0L,
                mutateMillis,
                configuration
            );
            this.relationshipsWritten = relationshipsWritten;
        }

        static class Builder extends AbstractResultBuilder<LinkPredictionPredictMutateProc.MutateResult> {

            @Override
            public LinkPredictionPredictMutateProc.MutateResult build() {
                return new LinkPredictionPredictMutateProc.MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    relationshipsWritten,
                    config.toMap()
                );
            }
        }
    }
}
