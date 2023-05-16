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

import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.AlgorithmSpecProgressTrackerProvider;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.Kmeans.KMEANS_DESCRIPTION;

@GdsCallable(name = "gds.beta.kmeans.write", description = KMEANS_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class KmeansWriteSpec implements AlgorithmSpec<Kmeans, KmeansResult, KmeansWriteConfig, Stream<WriteResult>, KmeansAlgorithmFactory<KmeansWriteConfig>> {

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
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            var returnColumns = executionContext.returnColumns();
            var builder = new WriteResult.Builder(
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
                .withNodeCount(graph.nodeCount())
                .withConfig(computationResult.config());


            try (ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
                var writeConcurrency = computationResult.config().writeConcurrency();
                var algorithm = computationResult.algorithm();
                var config = computationResult.config();

                NodePropertyExporter exporter =  executionContext.nodePropertyExporterBuilder()
                    .withIdMap(graph)
                    .withTerminationFlag(algorithm.getTerminationFlag())
                    .withProgressTracker(AlgorithmSpecProgressTrackerProvider.createProgressTracker(
                        name(),
                        graph.nodeCount(),
                        writeConcurrency,
                        executionContext
                    ))
                    .withArrowConnectionInfo(config.arrowConnectionInfo(), computationResult.graphStore().databaseId().databaseName())
                    .parallel(Pools.DEFAULT, writeConcurrency)
                    .build();

                LongNodePropertyValues properties;
                if (computationResult.result().isPresent()) {
                    var communities = computationResult.result().get().communities();
                    properties = new LongNodePropertyValues() {
                        @Override
                        public long nodeCount() {
                            return graph.nodeCount();
                        }

                        @Override
                        public long longValue(long nodeId) {
                            return communities.get(nodeId);
                        }
                    };
                } else {
                    properties = EmptyLongNodePropertyValues.INSTANCE;
                }

                exporter.write(
                    config.writeProperty(),
                    properties
                );
                builder.withNodePropertiesWritten(exporter.propertiesWritten());
            }

            return Stream.of(builder.build());
        };
    }

}
