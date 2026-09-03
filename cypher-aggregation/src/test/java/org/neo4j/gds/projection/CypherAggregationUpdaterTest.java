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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.progress.registration.EmptyTaskStore;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class CypherAggregationUpdaterTest {

    private final GraphStoreCatalogService graphStoreCatalogService = new GraphStoreCatalogService();

    private final DatabaseId databaseId = DatabaseId.random();

    @AfterEach
    void tearDown() {
        graphStoreCatalogService.removeAllLoadedGraphs(databaseId);
    }

    private LazyGraphImporter newLazyGraphImporter() {
        return new LazyGraphImporter(
            "neo4j",
            databaseId,
            ExecutingQueryProvider.empty(),
            QueryEstimator.empty(),
            Capabilities.WriteMode.LOCAL,
            graphStoreCatalogService,
            PlainSimpleRequestCorrelationId.create(),
            EmptyTaskStore.INSTANCE,
            Log.noOpLog()
        );
    }

    private CypherAggregationReducer newReducer(LazyGraphImporter lazyGraphImporter, ExtractNodeId extractNodeId) {
        return new CypherAggregationReducer(
            lazyGraphImporter,
            ProjectionMetricsService.DISABLED,
            databaseId,
            extractNodeId,
            CypherAggregationUpdater.InputValuesMapper.identity()
        );
    }

    private static AnyValue[] row(String graphName, long sourceNode, long targetNode) {
        return new AnyValue[]{
            Values.stringValue(graphName),
            Values.longValue(sourceNode),
            Values.longValue(targetNode),
            MapValue.EMPTY,      // dataConfig
            MapValue.EMPTY,      // configuration
            NoValue.NO_VALUE     // alphaMigrationConfig
        };
    }

    private static AnyValue[] row(String graphName, long sourceNode, AnyValue invalidTargetNode) {
        return new AnyValue[]{
            Values.stringValue(graphName),
            Values.longValue(sourceNode),
            invalidTargetNode,
            MapValue.EMPTY,
            MapValue.EMPTY,
            NoValue.NO_VALUE
        };
    }

    private static AnyValue[] rowWithReadConcurrencyOne(String graphName, long sourceNode, long targetNode) {
        var configBuilder = new MapValueBuilder(1);
        configBuilder.add("readConcurrency", Values.longValue(1));
        return new AnyValue[]{
            Values.stringValue(graphName),
            Values.longValue(sourceNode),
            Values.longValue(targetNode),
            MapValue.EMPTY,
            configBuilder.build(),
            NoValue.NO_VALUE
        };
    }

    @Test
    void shouldAggregateAcrossMorselCycles() throws Exception {
        try (
            var lazyGraphImporter = newLazyGraphImporter();
            var reducer = newReducer(lazyGraphImporter, new ExtractNodeId())
        ) {
            // one updater, as under the interpreted and pipelined-serial runtimes
            var updater = reducer.newUpdater();

            // morsel 1
            updater.update(row("g", 0, 1));
            updater.update(row("g", 1, 2));
            updater.applyUpdates();

            // morsel 2 (the thread-local batches must re-acquire their claims)
            updater.update(row("g", 2, 3));
            updater.applyUpdates();

            // morsel 3
            updater.update(row("g", 3, 0));

            var result = reducer.buildGraph();

            assertThat(result).isNotNull();
            assertThat(result.nodeCount()).isEqualTo(4);
            assertThat(result.relationshipCount()).isEqualTo(4);
        }
    }

    @Test
    void shouldAggregateFromConcurrentUpdaters() throws Exception {
        try (
            var lazyGraphImporter = newLazyGraphImporter();
            var reducer = newReducer(lazyGraphImporter, new ExtractNodeId())
        ) {
            // one updater per worker, as under the parallel runtime
            var updaterA = reducer.newUpdater();
            var updaterB = reducer.newUpdater();

            assertTimeoutPreemptively(java.time.Duration.ofSeconds(60), () -> {
                ExecutorService executor = Executors.newFixedThreadPool(2);
                try {
                    List<Future<?>> futures = List.of(
                        executor.submit(() -> {
                            updaterA.update(row("g", 0, 1));
                            updaterA.update(row("g", 1, 2));
                            updaterA.applyUpdates();
                            updaterA.update(row("g", 2, 3));
                            return null;
                        }),
                        executor.submit(() -> {
                            updaterB.update(row("g", 10, 11));
                            updaterB.update(row("g", 11, 12));
                            updaterB.applyUpdates();
                            updaterB.update(row("g", 12, 13));
                            return null;
                        })
                    );
                    for (Future<?> future : futures) {
                        future.get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    executor.shutdown();
                }

                var result = reducer.buildGraph();

                assertThat(result).isNotNull();
                assertThat(result.nodeCount()).isEqualTo(8);
                assertThat(result.relationshipCount()).isEqualTo(6);
            });
        }
    }

    @Test
    void shouldReleaseLeakedSessionsOnClose() throws Exception {
        // readConcurrency 1 -> a single pooled slot; a leaked claim would block
        // any further update until the claim timeout
        try (
            var lazyGraphImporter = newLazyGraphImporter();
            var reducer = newReducer(lazyGraphImporter, new ExtractNodeId())
        ) {
            var updater = reducer.newUpdater();
            updater.update(rowWithReadConcurrencyOne("g", 0, 1));

            // the worker fails mid-morsel; the kernel never calls applyUpdates
            assertThatThrownBy(() -> updater.update(row("g", 1, Values.stringValue("invalidID"))))
                .isInstanceOf(ProcedureException.class)
                .hasMessageContaining("The node has to be either a NODE or an INTEGER, but got String");

            // assuming this gets called by Neo4j on the error path
            reducer.close();

            // the failed updater's session was force-closed, so its slot is
            // available to a new updater
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), () -> {
                var nextUpdater = reducer.newUpdater();
                nextUpdater.update(rowWithReadConcurrencyOne("g", 1, 2));
            });
        }
    }

    @Test
    void shouldReturnCachedResultOnRepeatedBuilds() throws Exception {
        try (
            var lazyGraphImporter = newLazyGraphImporter();
            var reducer = newReducer(lazyGraphImporter, new ExtractNodeId())
        ) {
            var updater = reducer.newUpdater();
            updater.update(row("g", 0, 1));

            var firstResult = reducer.buildGraph();
            var secondResult = reducer.buildGraph();

            assertThat(secondResult).isSameAs(firstResult);
        }
    }

    @Test
    void shouldReturnNoValueWhenNothingWasAggregated() throws Exception {
        try (
            var lazyGraphImporter = newLazyGraphImporter();
            var reducer = newReducer(lazyGraphImporter, new ExtractNodeId())
        ) {
            // the kernel may call result() even when no row reached any updater
            assertThat(reducer.buildGraph()).isNull();
            assertThat(reducer.result()).isEqualTo(NoValue.NO_VALUE);
        }
    }
}
