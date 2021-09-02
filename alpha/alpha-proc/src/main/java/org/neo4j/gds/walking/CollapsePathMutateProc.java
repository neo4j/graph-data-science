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

import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.ImmutableComputationResult;
import org.neo4j.gds.MutateProc;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.impl.walking.CollapsePath;
import org.neo4j.gds.impl.walking.CollapsePathConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class CollapsePathMutateProc extends MutateProc<CollapsePath, Relationships, CollapsePathMutateProc.MutateResult, CollapsePathConfig> {

    @Procedure(name = "gds.alpha.collapsePath.mutate", mode = READ)
    @Description("")
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphNameOrConfig, configuration, true, false);
        return mutate(computationResult);
    }

    @Override
    protected ComputationResult<CollapsePath, Relationships, CollapsePathConfig> compute(
        Object graphNameOrConfig, Map<String, Object> configuration, boolean releaseAlgorithm, boolean releaseTopology
    ) {
        ImmutableComputationResult.Builder<CollapsePath, Relationships, CollapsePathConfig> builder = ImmutableComputationResult.builder();

        Pair<CollapsePathConfig, Optional<String>> input = processInput(
            graphNameOrConfig,
            configuration
        );
        var config = input.getOne();

        GraphStore graphStore;
        try (ProgressTimer timer = ProgressTimer.start(builder::createMillis)) {
            graphStore = getOrCreateGraphStore(input);
        }

        Graph[] graphs = config.relationshipTypes()
            .stream()
            .map(relType -> graphStore.getGraph(RelationshipType.of(relType)))
            .toArray(Graph[]::new);

        var allocationTracker = allocationTracker();

        CollapsePath algo = new CollapsePath(graphs, config, Pools.DEFAULT, allocationTracker);
        builder.algorithm(algo);

        try (ProgressTimer timer = ProgressTimer.start(builder::computeMillis)) {
            builder.result(algo.compute());
        }

        log.info(algoName() + ": overall memory usage %s", allocationTracker.getUsageString());

        algo.release();

        for (Graph graph : graphs) {
            graph.releaseTopology();
        }

       return builder
            .graphStore(graphStore)
            .graph(graphs[0])
            .algorithm(algo)
            .config(config)
            .build();
    }

    @Override
    protected CollapsePathConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return CollapsePathConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<CollapsePath, Relationships, CollapsePathConfig> computationResult
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            computationResult.graphStore().addRelationshipType(
                RelationshipType.of(computationResult.config().mutateRelationshipType()),
                Optional.empty(),
                Optional.empty(),
                computationResult.result()
            );
        }

        resultBuilder.withRelationshipsWritten(computationResult.result().topology().elementCount());
    }

    @SuppressWarnings("unused")
    public static class MutateResult {
        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long relationshipsWritten;

        public final Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long relationshipsWritten,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.relationshipsWritten = relationshipsWritten;
            this.configuration = configuration;
        }

        static class Builder extends AbstractResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    relationshipsWritten,
                    config.toMap()
                );
            }
        }
    }

    @Override
    protected AlgorithmFactory<CollapsePath, CollapsePathConfig> algorithmFactory() {
        throw new UnsupportedOperationException("CollapsePath does not support the AlgorithmFactory");
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<CollapsePath, Relationships, CollapsePathConfig> computeResult) {
        return new MutateResult.Builder();
    }
}
