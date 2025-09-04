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
import org.neo4j.gds.TestTaskStore;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Status;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductGraphAggregatorIT {

    @Test
    void shouldImportHighNodeIds() {
        var userName = "neo4j";
        var graphName = "graph";
        var databaseId = DatabaseId.random();

        var aggregator = new ProductGraphAggregator(
            databaseId,
            userName,
            WriteMode.LOCAL,
            QueryEstimator.empty(),
            ExecutingQueryProvider.empty(),
            ProjectionMetricsService.DISABLED,
            EmptyTaskStore.INSTANCE,
            Log.noOpLog()
        );

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

        assertThat(result.nodeCount()).isEqualTo(2);
        assertThat(result.relationshipCount()).isEqualTo(1);

        var graphStoreWithConfig = GraphStoreCatalog.get(userName, databaseId, graphName);
        var graphStore = graphStoreWithConfig.graphStore();

        assertThat(graphStore.nodes().toOriginalNodeId(0)).isEqualTo(source);
        assertThat(graphStore.nodes().toOriginalNodeId(1)).isEqualTo(target);
    }

    @ParameterizedTest(name = "graphName=`{1}`")
    @MethodSource("emptyGraphNames")
    void shouldFailOnEmptyGraphName(String emptyGraphName, String description) {

        TestTaskStore taskStore = new TestTaskStore();
        var aggregator = new ProductGraphAggregator(
            DatabaseId.random(),
            "neo4j",
            WriteMode.LOCAL,
            QueryEstimator.empty(),
            ExecutingQueryProvider.empty(),
            ProjectionMetricsService.DISABLED,
            taskStore,
            Log.noOpLog()
        );

        assertThatIllegalArgumentException().isThrownBy(() ->
            aggregator.projectNextRelationship(
                Values.stringValue(emptyGraphName),
                Values.longValue(1L),
                Values.longValue(2L),
                MapValue.EMPTY,
                MapValue.EMPTY,
                NoValue.NO_VALUE
            )).withMessageContaining("`graphName` can not be null or blank");

        assertThat(taskStore.tasksSeen()).isEmpty();
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
        TestTaskStore taskStore = new TestTaskStore();
        var aggregator = new ProductGraphAggregator(
            DatabaseId.random(),
            "neo4j",
            WriteMode.LOCAL,
            QueryEstimator.empty(),
            ExecutingQueryProvider.empty(),
            ProjectionMetricsService.DISABLED,
            taskStore,
            Log.noOpLog()
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
