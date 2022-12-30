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
package org.neo4j.gds.walking;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.MutateProc;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.beta.walking.CollapsePath;
import org.neo4j.gds.beta.walking.CollapsePathAlgorithmFactory;
import org.neo4j.gds.beta.walking.CollapsePathConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.walking.CollapsePathMutateProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.collapsePath.mutate", description = DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class CollapsePathMutateProc extends MutateProc<CollapsePath, SingleTypeRelationshipImportResult, CollapsePathMutateProc.MutateResult, CollapsePathConfig> {

    static final String DESCRIPTION = "Collapse Path algorithm is a traversal algorithm capable of creating relationships between the start and end nodes of a traversal";

    @Procedure(name = "gds.beta.collapsePath.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration, true, false);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    protected CollapsePathConfig newConfig(String username, CypherMapWrapper config) {
        return CollapsePathConfig.of(config);
    }

    @Override
    public MutateComputationResultConsumer<CollapsePath, SingleTypeRelationshipImportResult, CollapsePathConfig, MutateResult> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<CollapsePath, SingleTypeRelationshipImportResult, CollapsePathConfig> computationResult,
                ExecutionContext executionContext
            ) {
                try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
                    computationResult.graphStore().addRelationshipType(
                        RelationshipType.of(computationResult.config().mutateRelationshipType()),
                        computationResult.result()
                    );
                }

                resultBuilder.withRelationshipsWritten(computationResult.result().topology().elementCount());
            }
        };
    }

    @SuppressWarnings("unused")
    public static class MutateResult {
        public final long preProcessingMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long relationshipsWritten;

        public final Map<String, Object> configuration;

        MutateResult(
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long relationshipsWritten,
            Map<String, Object> configuration
        ) {
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.relationshipsWritten = relationshipsWritten;
            this.configuration = configuration;
        }

        static class Builder extends AbstractResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    preProcessingMillis,
                    computeMillis,
                    mutateMillis,
                    relationshipsWritten,
                    config.toMap()
                );
            }
        }
    }

    @Override
    public GraphStoreAlgorithmFactory<CollapsePath, CollapsePathConfig> algorithmFactory() {
        return new CollapsePathAlgorithmFactory();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<CollapsePath, SingleTypeRelationshipImportResult, CollapsePathConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder();
    }
}
