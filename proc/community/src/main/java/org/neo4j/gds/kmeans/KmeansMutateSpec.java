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
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.Kmeans.KMEANS_DESCRIPTION;

@GdsCallable(name = "gds.beta.kmeans.mutate", description = KMEANS_DESCRIPTION, executionMode = ExecutionMode.MUTATE_NODE_PROPERTY)
public class KmeansMutateSpec implements AlgorithmSpec<Kmeans, KmeansResult, KmeansMutateConfig, Stream<MutateResult>, KmeansAlgorithmFactory<KmeansMutateConfig>> {

    @Override
    public String name() {
        return "KmeansMutate";
    }

    @Override
    public KmeansAlgorithmFactory<KmeansMutateConfig> algorithmFactory() {
        return new KmeansAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KmeansMutateConfig> newConfigFunction() {
        return (__, config) -> KmeansMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        MutatePropertyComputationResultConsumer.MutateNodePropertyListFunction<Kmeans, KmeansResult, KmeansMutateConfig> mutateConfigNodePropertyListFunction =
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                computationResult.result()
                    .map(KmeansResult::communities)
                    .map(HugeIntArray::asNodeProperties)
                    .orElse(EmptyLongNodePropertyValues.INSTANCE)
            ));
        return new MutatePropertyComputationResultConsumer<>(
            mutateConfigNodePropertyListFunction,
            this::resultBuilder
        );
    }

    @NotNull
    private AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<Kmeans, KmeansResult, KmeansMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var returnColumns = executionContext.returnColumns();
        var builder = new MutateResult.Builder(
            returnColumns,
            computationResult.config().concurrency()
        );

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

        builder
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withNodeCount(computationResult.graph().nodeCount())
            .withConfig(computationResult.config());

        return builder;
    }
}
