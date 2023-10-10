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
package org.neo4j.gds.kmeans;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.procedures.community.kmeans.KmeansWriteResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.Kmeans.KMEANS_DESCRIPTION;

@GdsCallable(name = "gds.kmeans.write", aliases = {"gds.beta.kmeans.write"}, description = KMEANS_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class KmeansWriteSpec implements AlgorithmSpec<Kmeans, KmeansResult, KmeansWriteConfig, Stream<KmeansWriteResult>, KmeansAlgorithmFactory<KmeansWriteConfig>> {

    @Override
    public String name() {
        return "KmeansWrite";
    }

    @Override
    public KmeansAlgorithmFactory<KmeansWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KmeansAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KmeansWriteConfig> newConfigFunction() {
        return (__, config) -> KmeansWriteConfig.of(config);
    }

        @Override
        public ComputationResultConsumer<Kmeans, KmeansResult, KmeansWriteConfig, Stream<KmeansWriteResult>> computationResultConsumer() {
            return new WriteNodePropertiesComputationResultConsumer<>(
                this::resultBuilder,
                computationResult -> {
                    var nodePropertyValues  = computationResult.result()
                        .map(KmeansResult::communities)
                        .map(NodePropertyValuesAdapter::adapt)
                        .orElse(EmptyLongNodePropertyValues.INSTANCE);

                    return List.of(ImmutableNodeProperty.of(
                    computationResult.config().writeProperty(),
                        nodePropertyValues));
                },
                name()
            );
        }

    @NotNull
    private AbstractResultBuilder<KmeansWriteResult> resultBuilder(
        ComputationResult<Kmeans, KmeansResult, KmeansWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new KmeansWriteResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );
        var returnColumns = executionContext.returnColumns();
        computationResult.result().ifPresent(result -> {
            if (returnColumns.contains("centroids")) {
                builder.withCentroids(KmeansProcHelper.arrayMatrixToListMatrix(result.centers()));
            }
            if (returnColumns.contains("averageDistanceToCentroid")) {
                builder.withAverageDistanceToCentroid(result.averageDistanceToCentroid());
            }

            if (returnColumns.contains("averageSilhouette")) {
                builder.withAverageSilhouette(result.averageSilhouette());
            }
            builder.withCommunityFunction(result.communities()::get);

        });

        return builder
            .withConfig(computationResult.config());
    }

}
