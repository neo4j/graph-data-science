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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.community.InstrumentedCommunityAlgorithms;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.execution.machinery.DefaultAlgorithmProcessingFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.termination.TerminatedException;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class TriangleCentralityAsyncProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE (a:A)-[:T]->(b:A), (b)-[:T]->(c:A), (c)-[:T]->(a), (a)-[:T]->(leaf:A)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class);
        runQuery("CALL gds.graph.project('graph', 'A', {T: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldReturnFutureBeforeQueuedComputationCompletes() throws Exception {
        var gate = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            // Occupy the only worker so completion cannot race the assertion.
            // Bound the wait so a blocking-launch regression can still finish and fail.
            executor.submit(() -> {
                gate.await(30, TimeUnit.SECONDS);
                return null;
            });
            try {
                var future = algorithms(executor, TerminationFlag.RUNNING_TRUE)
                    .<TriangleCentralityResult, Void>triangleCentrality(
                        GraphName.parse("graph"),
                        TriangleCentralityBaseConfig.of(CypherMapWrapper.create(Map.of("concurrency", 1))),
                        Optional.empty(),
                        (resources, result, timings, metadata) -> result.orElseThrow()
                    );

                assertThat(future).isNotDone();
                gate.countDown();

                var result = future.get(10, TimeUnit.SECONDS);
                assertThat(result.globalTriangleCount()).isEqualTo(1);
                assertThat(result.centralities().toArray()).containsExactly(1.0, 1.0, 1.0, 1.0);
            } finally {
                gate.countDown();
            }
        }
    }

    @Test
    void shouldPropagateTerminationWithoutApplyingSideEffects() {
        // A failed computation must remain an exceptional future; treating it as a successful empty
        // result could run mutate/write side effects even though the operation was cancelled.
        var sideEffectApplied = new AtomicBoolean();
        SideEffect<TriangleCentralityResult, Void> sideEffect = (resources, result) -> {
            sideEffectApplied.set(true);
            return Optional.empty();
        };

        try (var executor = Executors.newSingleThreadExecutor()) {
            var future = algorithms(executor, TerminationFlag.STOP_RUNNING)
                .<TriangleCentralityResult, Void>triangleCentrality(
                    GraphName.parse("graph"),
                    TriangleCentralityBaseConfig.of(CypherMapWrapper.create(Map.of("concurrency", 1))),
                    Optional.of(sideEffect),
                    (resources, result, timings, metadata) -> result.orElseThrow()
                );

            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(TerminatedException.class);
            assertThat(sideEffectApplied).isFalse();
        }
    }

    private InstrumentedCommunityAlgorithms algorithms(ExecutorService executor, TerminationFlag terminationFlag) {
        var log = mock(Log.class);
        var dependencies = RequestScopedDependencies.builder()
            .correlationId(PlainSimpleRequestCorrelationId.create())
            .databaseId(DatabaseId.of(db.databaseName()))
            .graphLoaderContext(GraphLoaderContext.NULL_CONTEXT)
            .taskRegistryFactory(TaskRegistryFactory.empty())
            .terminationFlag(terminationFlag)
            .user(new User(getUsername(), false))
            .build();
        var processingFacade = DefaultAlgorithmProcessingFacade.create(
            log,
            new GraphStoreCatalogService(),
            executor,
            MemoryGuard.DISABLED,
            AlgorithmMetricsService.DISABLED,
            TelemetryLogger.DISABLED
        );
        var progressTrackerCreator = ProgressTrackerCreator.construct(
            log,
            new LoggerForProgressTrackingAdapter(log),
            dependencies.correlationId(),
            dependencies.taskRegistryFactory()
        );

        return InstrumentedCommunityAlgorithms.create(
            log,
            progressTrackerCreator,
            terminationFlag,
            new CommunityAlgorithmsEstimationModeBusinessFacade(mock(AlgorithmEstimationTemplate.class)),
            new LaunchConvenience(processingFacade, dependencies)
        );
    }
}
