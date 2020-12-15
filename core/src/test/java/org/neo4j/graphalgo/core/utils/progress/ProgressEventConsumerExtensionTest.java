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
package org.neo4j.graphalgo.core.utils.progress;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Level;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.stream.Stream;

class ProgressEventConsumerExtensionTest extends BaseTest {

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(GraphDatabaseSettings.store_internal_log_level, Level.DEBUG);
        // make sure that we 1) have our extension under test and 2) have it only once
        builder.removeExtensions(ex -> ex instanceof ProgressEventConsumerExtension);
        builder.addExtension(new ProgressEventConsumerExtension());
    }

    @Test
    void test() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, AlgoProc.class, ProgressProc.class);
        QueryRunner.runQuery(db, "CALL gds.test.algo");
        var result = QueryRunner.runQuery(db, "CALL gds.test.log", Result::resultAsString);
        System.out.println(result);
    }

    public static class AlgoProc {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.algo")
        public Stream<Bar> foo() {
            progress.addLogEvent("foo", "hello from any algo proc");
            return Stream.empty();
        }
    }

    public static class ProgressProc {
        @Context
        public ProgressEventConsumer progress;

        @Procedure("gds.test.log")
        public Stream<Bar> foo() {
            return progress.query("").stream().map(LogEvent::message).map(Bar::new);
        }
    }

    public static class Bar {
        public final String field;

        public Bar(String field) {this.field = field;}
    }
}
