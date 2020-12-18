/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;


import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPFactory;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.GraphGenerateProc;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.LogEvent;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventConsumerExtension;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.ProgressFeatureSettings;
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
        builder.removeExtensions(ex -> ex instanceof ProgressEventConsumerExtension);
        builder.addExtension(new ProgressEventConsumerExtension(scheduler));
    }

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            AlgoProc.class,
            ListProgressProc.class,
            ProgressLoggingAlgoProc.class,
            GraphGenerateProc.class,
            ProgressLoggingTestFastRP.class
        );
    }

    @Test
    void canListProgressEvent() {
        runQuery("CALL gds.test.algo('1')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var result = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.<String>columnAs("message").stream().collect(toList())
        );
        assertThat(result).containsExactly("hello 1");
    }

    @Test
    void listOnlyLastProgressEvent() {
        runQuery("CALL gds.test.algo('1')");
        runQuery("CALL gds.test.algo('2')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var result = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.<String>columnAs("message").stream().collect(toList())
        );
        assertThat(result).containsExactly("hello 2");
    }

    @Test
    void progressIsListedFilteredByUser() {
        runQuery("Alice", "CALL gds.test.algo('Alice')");
        runQuery("Bob", "CALL gds.test.algo('Bob')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var aliceResult = runQuery(
            "Alice",
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.<String>columnAs("message").stream().collect(toList())
        );
        assertThat(aliceResult).containsExactly("hello Alice");
        var bobResult = runQuery(
            "Bob",
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.<String>columnAs("message").stream().collect(toList())
        );
        assertThat(bobResult).containsExactly("hello Bob");
    }

    @Test
    void progressIsListedWithOneEventPerSource() {
        runQuery("CALL gds.test.algo('foo', 'pagerank')");
        runQuery("CALL gds.test.algo('bar', 'wcc')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        List<Map<String, Object>> expected1 = List.of(
            Map.of("source", "pagerank", "message", "hello foo"),
            Map.of("source", "wcc", "message", "hello bar")
        );
        var result1 = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.stream().collect(Collectors.toList())
        );

        assertThat(result1).containsExactlyInAnyOrderElementsOf(expected1);

        // causing new events, the newer events will be returned for the same keys
        runQuery("CALL gds.test.algo('apa', 'pagerank')");
        runQuery("CALL gds.test.algo('bepa', 'wcc')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        List<Map<String, Object>> expected2 = List.of(
            Map.of("source", "pagerank", "message", "hello apa"),
            Map.of("source", "wcc", "message", "hello bepa")
        );
        var result2 = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.stream().collect(Collectors.toList())
        );

        assertThat(result2).containsExactlyInAnyOrderElementsOf(expected2);
    }

    public static class AlgoProc extends BaseProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.algo")
        public Stream<Bar> foo(
            @Name(value = "param") String param,
            @Name(value = "source", defaultValue = "test") String source
        ) {
            progress.addLogEvent(source, "hello " + param);
            return Stream.empty();
        }
    }

    @Test
    void progressLoggerShouldEmitProgressEvents() {
        runQuery("CALL gds.test.logging_algo('foo', 'pagerank')");
        runQuery("CALL gds.test.logging_algo('bar', 'wcc')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        List<Map<String, Object>> expected = List.of(
            Map.of("source", "pagerank", "message", "[main] pagerank 100% hello foo"),
            Map.of("source", "wcc", "message", "[main] wcc 100% hello bar")
        );
        var result = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.stream().collect(Collectors.toList())
        );

        assertThat(result)
            .hasSize(2)
            .element(0, InstanceOfAssertFactories.map(String.class, String.class))
            .hasEntrySatisfying("message", v -> assertThat(v).contains("wcc 100% hello bar"))
            .hasEntrySatisfying("source", v -> assertThat(v).isEqualTo("wcc"));
        assertThat(result)
            .element(1, InstanceOfAssertFactories.map(String.class, String.class))
            .hasEntrySatisfying("message", v -> assertThat(v).contains("pagerank 100% hello foo"))
            .hasEntrySatisfying("source", v -> assertThat(v).isEqualTo("pagerank"));
    }

    @Test
    void progressLoggerShouldEmitProgressEventsOnActualAlgo() {
        runQuery("CALL gds.beta.graph.generate('foo', 100, 5)");
        runQuery("CALL gds.test.fakerp('foo', {embeddingDimension: 42})");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        List<Map<String, Object>> result = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.stream().collect(Collectors.toList())
        );

        assertThat(result).hasSize(1)
            .element(0, InstanceOfAssertFactories.map(String.class, String.class))
            .hasEntrySatisfying("message", v -> assertThat(v).contains("100%"))
            .hasEntrySatisfying("source", v -> assertThat(v).isEqualTo("FastRP"));
    }

    @Test
    void progressLoggerShouldClearProgressEventsOnLogFinish() {
        runQuery("CALL gds.beta.graph.generate('foo', 100, 5)");
        runQuery("CALL gds.test.fastrp('foo', {embeddingDimension: 42})");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        List<Map<String, Object>> result = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN source, message",
            r -> r.stream().collect(Collectors.toList())
        );

        assertThat(result).isEmpty();
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
                public void addLogEvent(String id, String message) {
                    tracker.addLogEvent(id, message);
                }

                @Override
                public void addLogEvent(String id, String message, double progress) {
                    tracker.addLogEvent(id, message, progress);
                }

                @Override
                public void addLogEvent(LogEvent event) {
                    tracker.addLogEvent(event);
                }

                @Override
                public void release(String id) {
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
                    Graph graph, FastRPStreamConfig configuration, AllocationTracker tracker, Log log,
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

                    return new FastRP(
                        graph,
                        configuration,
                        progressLogger,
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
