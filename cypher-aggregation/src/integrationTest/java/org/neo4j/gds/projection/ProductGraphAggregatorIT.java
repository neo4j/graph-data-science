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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.progress.registration.EmptyTaskStore;
import org.neo4j.gds.progress.registration.PerDatabaseTaskStore;
import org.neo4j.gds.progress.registration.TaskStore;
import org.neo4j.gds.progress.tasks.Status;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class ProductGraphAggregatorIT {

    @Test
    void shouldImportHighNodeIds() throws Exception {
        var userName = "neo4j";
        var graphName = "graph";
        var databaseId = DatabaseId.random();

        var graphStoreCatalogService = new GraphStoreCatalogService();
        try (
            var aggregator = new ProductGraphAggregator(
                databaseId,
                userName,
                Capabilities.WriteMode.LOCAL,
                QueryEstimator.empty(),
                ExecutingQueryProvider.empty(),
                graphStoreCatalogService,
                ProjectionMetricsService.DISABLED,
                EmptyTaskStore.INSTANCE,
                Log.noOpLog(),
                PlainSimpleRequestCorrelationId.create()
            )
        ) {

            long source = 1L << 50;
            long target = (1L << 50) + 1;

            aggregator.projectNextRelationship(
                Values.stringValue(graphName),
                Values.longValue(source),
                Values.longValue(target),
                MapValue.EMPTY,
                MapValue.EMPTY,
                NoValue.NO_VALUE
            );

            var result = aggregator.buildGraph();

            assertThat(result)
                .isNotNull()
                .as("ProjectionResult should be present ")
                .satisfies(
                    projectionResult -> assertThat(projectionResult.nodeCount())
                        .as("result should have 2 nodes")
                        .isEqualTo(2),
                    projectionResult -> assertThat(projectionResult.relationshipCount())
                        .as("result should have 1 relationship")
                        .isEqualTo(1)
                );

            var graphStore = graphStoreCatalogService.getGraphStoreCatalogEntry(
                CatalogRequest.of(new User(userName, false), databaseId),
                GraphName.parse(graphName)
            ).graphStore();

            assertThat(graphStore.nodes().toOriginalNodeId(0)).isEqualTo(source);
            assertThat(graphStore.nodes().toOriginalNodeId(1)).isEqualTo(target);
        }
    }

    @ParameterizedTest(name = "graphName=`{1}`")
    @MethodSource("emptyGraphNames")
    void shouldFailOnEmptyGraphName(String emptyGraphName, String description) throws Exception {
        var taskStore = mock(TaskStore.class);
        try (
            var aggregator = new ProductGraphAggregator(
                DatabaseId.random(),
                "neo4j",
                Capabilities.WriteMode.LOCAL,
                QueryEstimator.empty(),
                ExecutingQueryProvider.empty(),
                new GraphStoreCatalogService(),
                ProjectionMetricsService.DISABLED,
                taskStore,
                Log.noOpLog(),
                PlainSimpleRequestCorrelationId.create()
            )
        ) {
            assertThatIllegalArgumentException().isThrownBy(() ->
                aggregator.projectNextRelationship(
                    Values.stringValue(emptyGraphName),
                    Values.longValue(1L),
                    Values.longValue(2L),
                    MapValue.EMPTY,
                    MapValue.EMPTY,
                    NoValue.NO_VALUE
                )).withMessageContaining("`graphName` can not be null or blank");
        }

        verifyNoInteractions(taskStore);
    }

    private static Stream<Arguments> emptyGraphNames() {
        return Stream.of(
            Arguments.of("", "empty"),
            Arguments.of("\t", "tab"),
            Arguments.of("\n", "new line"),
            Arguments.of("   ", "spaces")
        );
    }

    @Test
    void shouldFailTaskOnFailure() throws Exception {
        var taskStore = PerDatabaseTaskStore.create(Duration.ofMinutes(5));
        var aggregator = new ProductGraphAggregator(
            DatabaseId.random(),
            "neo4j",
            Capabilities.WriteMode.LOCAL,
            QueryEstimator.empty(),
            ExecutingQueryProvider.empty(),
            new GraphStoreCatalogService(),
            ProjectionMetricsService.DISABLED,
            taskStore,
            Log.noOpLog(),
            PlainSimpleRequestCorrelationId.create()
        );

        assertThatThrownBy(() ->
            aggregator.update(new AnyValue[] {
                Values.stringValue("my-graph"),
                Values.longValue(1L),
                Values.stringValue("invalidID"),
                MapValue.EMPTY,
                MapValue.EMPTY,
                NoValue.NO_VALUE }
            ))
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The node has to be either a NODE or an INTEGER, but got String");

        // assuming this gets called by Neo4j
        aggregator.close();

        assertThat(taskStore.query())
            .map(i -> i.task().status())
            .containsExactly(Status.FAILED);
    }
}
