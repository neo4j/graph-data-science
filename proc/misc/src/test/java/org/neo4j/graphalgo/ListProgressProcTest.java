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


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventConsumerExtension;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.ProgressFeatureSettings;
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
        builder.removeExtensions(ex -> ex instanceof ProgressEventConsumerExtension);
        builder.addExtension(new ProgressEventConsumerExtension(scheduler));
    }

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, AlgoProc.class, ListProgressProc.class);
    }

    @Test
    void canListProgressEvent() {
        runQuery("CALL gds.test.algo('1')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);
        var result = runQuery(
            "CALL gds.beta.listProgress() YIELD source, message RETURN id, message",
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
            "CALL gds.beta.listProgress() YIELD source, message RETURN id, message",
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
            "CALL gds.beta.listProgress() YIELD source, message RETURN id, message",
            r -> r.<String>columnAs("message").stream().collect(toList())
        );
        assertThat(aliceResult).containsExactly("hello Alice");
        var bobResult = runQuery(
            "Bob",
            "CALL gds.beta.listProgress() YIELD source, message RETURN id, message",
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
            @Name(value = "source", defaultValue = "test") String id
        ) {
            progress.addLogEvent(id, "hello " + param);
            return Stream.empty();
        }

        public static class Bar {
            public final String field;

            public Bar(String field) {this.field = field;}
        }
    }

}
