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
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.utils.RenamesCurrentThread;
import org.neo4j.gds.core.utils.progress.ProgressEventExtension;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPFactory;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.logging.Level;
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
        builder.removeExtensions(ex -> ex instanceof ProgressEventExtension);
        builder.addExtension(new ProgressEventExtension(scheduler));
    }

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ListProgressProc.class,
            GraphGenerateProc.class,
            ProgressLoggingTestProc.class,
            ProgressLoggingTestFastRP.class
        );
    }

    @Test
    void canListProgressEvent() {
        runQuery("CALL gds.test.pl('foo')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var result = runQuery(
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
            r -> r.<String>columnAs("taskName").stream().collect(toList())
        );
        assertThat(result).containsExactly("foo");
    }

    @Test
    void listOnlyLastProgressEvent() {
        runQuery("CALL gds.test.pl('foo')");
        runQuery("CALL gds.test.pl('bar')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var progressEvents = runQuery(
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(progressEvents).hasSize(2);
        assertThat(progressEvents)
            .containsExactlyInAnyOrder(
                Map.of("taskName","foo"),
                Map.of("taskName", "bar")
            );
    }

    @Test
    void progressIsListedFilteredByUser() {
        runQuery("Alice", "CALL gds.test.pl('foo')");
        runQuery("Bob", "CALL gds.test.pl('bar')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        var aliceResult = runQuery(
            "Alice",
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(aliceResult).containsExactlyInAnyOrder(Map.of("taskName", "foo"));

        var bobResult = runQuery(
            "Bob",
            "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
            r -> r.stream().collect(Collectors.toList())
        );
        assertThat(bobResult).containsExactlyInAnyOrder(Map.of("taskName", "bar"));
    }

    @Test
    void progressLoggerShouldEmitProgressEventsOnActualAlgoButClearProgressEventsOnLogFinish() {
        try (var ignored = RenamesCurrentThread.renameThread("Test worker")) {
            runQuery("CALL gds.beta.graph.generate('foo', 100, 5)");
            runQuery("CALL gds.test.fakerp('foo', {embeddingDimension: 42})");
            scheduler.forward(100, TimeUnit.MILLISECONDS);

            List<Map<String, Object>> result = runQuery(
                "CALL gds.beta.listProgress() YIELD taskName RETURN taskName",
                r -> r.stream().collect(Collectors.toList())
            );

            assertThat(result).hasSize(1)
                .element(0, InstanceOfAssertFactories.map(String.class, String.class))
                .hasEntrySatisfying("taskName", v -> assertThat(v).isEqualTo("FastRP"));
        }
    }

    public static class ProgressLoggingTestProc extends BaseProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.pl")
        public Stream<Bar> foo(
            @Name(value = "taskName") String taskName
        ) {
            progress.addTaskProgressEvent(Tasks.leaf(taskName));
            return Stream.empty();
        }
    }

    public static class ProgressLoggingTestFastRP extends FastRPStreamProc {
        @Context
        public ProgressEventTracker progressEventTracker;

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
            var tracker = this.progressEventTracker;
            this.progressEventTracker = new ProgressEventTracker() {
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
                this.progressEventTracker = tracker;
            }
        }

        @Override
        protected AlgorithmFactory<FastRP, FastRPStreamConfig> algorithmFactory() {
            return new FastRPFactory<>();
        }
    }

    public static class Bar {
        public final String field;

        public Bar(String field) {this.field = field;}
    }
}
