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
package org.neo4j.gds;


import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.RenamesCurrentThread;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.ProgressEventHandlerExtension;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPFactory;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class ListProgressProcTest extends BaseTest {

    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(GraphDatabaseSettings.store_internal_log_level, Level.DEBUG);
        builder.setConfig(ProgressFeatureSettings.progress_tracking_enabled, true);
        // make sure that we 1) have our extension under test and 2) have it only once
        builder.removeExtensions(ex -> ex instanceof ProgressEventHandlerExtension);
        builder.addExtension(new ProgressEventHandlerExtension(scheduler));
    }

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            ProgressLoggingAlgoProc.class,
            GraphGenerateProc.class,
            ProgressLoggingTestFastRP.class
        );
    }

    @Test
    void canListProgressEvent() {
        runQuery("CALL gds.test.pl('myAlgo', 'foo')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var result = runQuery(
            "CALL gds.beta.listProgress() YIELD message RETURN message",
            r -> r.<String>columnAs("message").stream().collect(toList())
        );
        assertThat(result).containsExactly("foo");
    }

    @Test
    void listOnlyLastProgressEvent() {
        runQuery("CALL gds.test.pl('myAlgo1', 'foo', 'bar', 'baz')");
        runQuery("CALL gds.test.pl('myAlgo2', 'quux', 'frazzle')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var progressEvents = runQuery(
            "CALL gds.beta.listProgress() YIELD taskName, message RETURN taskName, message",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(progressEvents).hasSize(2);
        assertThat(progressEvents)
            .containsExactlyInAnyOrder(
                Map.of("taskName", "myAlgo1", "message", "baz"),
                Map.of("taskName", "myAlgo2", "message", "frazzle")
            );
    }

    @Test
    void progressIsListedFilteredByUser() {
        runQuery("Alice", "CALL gds.test.pl('myAlgo', 'foo')");
        runQuery("Bob", "CALL gds.test.pl('myAlgo','bar')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        var aliceResult = runQuery(
            "Alice",
            "CALL gds.beta.listProgress() YIELD taskName, message RETURN taskName, message",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(aliceResult).containsExactlyInAnyOrder(Map.of("taskName", "myAlgo", "message", "foo"));

        var bobResult = runQuery(
            "Bob",
            "CALL gds.beta.listProgress() YIELD taskName, message RETURN taskName, message",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(bobResult).containsExactlyInAnyOrder(Map.of("taskName", "myAlgo", "message", "bar"));
    }

    @Test
    void progressLoggerShouldEmitProgressEvents() {
        try (var ignored = RenamesCurrentThread.renameThread("Test worker")) {
            runQuery("CALL gds.test.logging_algo('foo', 'pagerank')");
            runQuery("CALL gds.test.logging_algo('bar', 'wcc')");
            scheduler.forward(100, TimeUnit.MILLISECONDS);

            var result = runQuery(
                "CALL gds.beta.listProgress() YIELD taskName, message RETURN taskName, message",
                r -> r.stream().collect(Collectors.toList())
            );

            assertThat(result).hasSize(2);

            assertThat(result).contains(Map.of("taskName", "pagerank", "message", "[Test worker] pagerank 100% hello foo"));
            assertThat(result).contains(Map.of("taskName", "wcc", "message", "[Test worker] wcc 100% hello bar"));
        }
    }

    @Test
    void progressLoggerShouldEmitProgressEventsOnActualAlgoButClearProgressEventsOnLogFinish() {
        try (var ignored = RenamesCurrentThread.renameThread("Test worker")) {
            runQuery("CALL gds.beta.graph.generate('foo', 100, 5)");
            runQuery("CALL gds.test.fakerp('foo', {embeddingDimension: 42})");
            scheduler.forward(100, TimeUnit.MILLISECONDS);

            List<Map<String, Object>> result = runQuery(
                "CALL gds.beta.listProgress() YIELD taskName, message RETURN taskName, message",
                r -> r.stream().collect(Collectors.toList())
            );

            assertThat(result).hasSize(1)
                .element(0, InstanceOfAssertFactories.map(String.class, String.class))
                .hasEntrySatisfying("message", v -> assertThat(v).isEqualTo("[Test worker] FastRP FastRP :: Finished"))
                .hasEntrySatisfying("taskName", v -> assertThat(v).isEqualTo("FastRP"));
        }
    }

    public static class ProgressLoggingAlgoProc extends BaseProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.logging_algo")
        public Stream<Bar> bar(
            @Name(value = "param") String param,
            @Name(value = "source", defaultValue = "test") String source
        ) {
            var progressLogger = new BatchingProgressLogger(log, 1, source, 2, progress);
            progressLogger.logProgress(() -> "hello " + param);
            return Stream.empty();
        }
    }

    public static class ProgressLoggingTestFastRP extends FastRPStreamProc {
        @Context
        public ProgressEventTracker progressTracker;

        @Override
        @Procedure("gds.test.fastrp")
        public Stream<FastRPStreamProc.StreamResult> stream(
            @Name(value = "graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return super.stream(graphNameOrConfig, configuration);
        }

        @Procedure("gds.test.fakerp")
        public Stream<FastRPStreamProc.StreamResult> fakeStream(
            @Name(value = "graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            var tracker = this.progressTracker;
            this.progressTracker = new ProgressEventTracker() {
                @Override
                public void addTaskProgressEvent(Task task) {
                    tracker.addTaskProgressEvent(task);
                }

                @Override
                public void release() {
                    // skip the release because we want to observe the messages after the algo is done
                }
            };
            try {
                return super.stream(graphNameOrConfig, configuration);
            } finally {
                this.progressTracker = tracker;
            }
        }

        @Override
        protected AlgorithmFactory<FastRP, FastRPStreamConfig> algorithmFactory() {

            return new FastRPFactory<>() {

                @Override
                public FastRP build(
                    Graph graph,
                    FastRPStreamConfig configuration,
                    AllocationTracker tracker,
                    Log log,
                    ProgressEventTracker eventTracker
                ) {
                    var progressLogger = new BatchingProgressLogger(
                        log,
                        graph.nodeCount(),
                        "FastRP",
                        configuration.concurrency(),
                        // use the field, not the provided one
                        progressTracker
                    );

                    var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger);

                    var featureExtractors = FeatureExtraction.propertyExtractors(graph, configuration.featureProperties());
                    return new FastRP(
                        graph,
                        configuration,
                        featureExtractors,
                        progressTracker,
                        tracker
                    );
                }
            };
        }
    }

    public static class Bar {
        public final String field;

        public Bar(String field) {this.field = field;}
    }
}
