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
package org.neo4j.gds.core;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.SplittableRandom;

@ExtendWith(RandomGraphTestCase.TestWatcherExtension.class)
public abstract class RandomGraphTestCase extends BaseTest {

    static final int NODE_COUNT = 100;

    static class TestWatcherExtension implements AfterEachCallback {

        // taken from org.neo4j.test.extension.DbmsSupportController
        private static final ExtensionContext.Namespace DBMS_NAMESPACE = ExtensionContext.Namespace.create(
            "org",
            "neo4j",
            "dbms"
        );

        // taken from org.neo4j.test.extension.DbmsSupportController
        private static final String DBMS_KEY = "service";

        TestWatcherExtension() {}

        @Override
        public void afterEach(ExtensionContext context) {
            var executionException = context.getExecutionException();
            executionException.flatMap(throwable -> getDbms(context))
                .map(dbms -> dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME))
                .ifPresent(this::dumpGraph);
        }

        Optional<DatabaseManagementService> getDbms(ExtensionContext context) {
            return Optional.ofNullable(context.getStore(DBMS_NAMESPACE).get(DBMS_KEY, DatabaseManagementService.class));
        }

        @SuppressForbidden(reason = "this is supposed to use sys.out")
        void dumpGraph(GraphDatabaseService db) {
            try {
                PrintWriter pw = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
                pw.println("Generated graph to reproduce any errors:");
                pw.println();
                CypherExporter.export(pw, db);
            } catch (Exception e) {
                System.err.println("Error exporting graph " + e.getMessage());
            }
        }
    }

    @BeforeEach
    protected void setupGraph() {
        buildGraph(NODE_COUNT);
    }

    public final void buildGraph(int nodeCount) {
        var random = new SplittableRandom();
        var label1 = Label.label("Label");
        var label2 = Label.label("Label2");
        var type = RelationshipType.withName("TYPE");

        try (var tx = db.beginTx()) {
            // 1. Create nodeCount nodes with a label
            //    For each, 50% chance it gets a second label
            for (int i = 0; i < nodeCount; i++) {
                var node = tx.createNode(label1);
                if (random.nextBoolean()) {
                    node.addLabel(label2);
                }
            }
            // 2. For the first thousand random pairs of nodes
            //    For 10% of cases, create a relationship with some weight
            final var count = new MutableInt();
            var limit = 1000;
            var chance = 0.1;
            tx.getAllNodes().forEach(
                n1 -> tx.getAllNodes().forEach(
                    n2 -> {
                        if (count.getValue() < limit) {
                            if (random.nextDouble() < chance) {
                                var rel = n1.createRelationshipTo(n2, type);
                                rel.setProperty("weight", Math.ceil(10 * random.nextDouble()) / 10);
                            }
                            count.increment();
                        }

                    }
                )
            );

            tx.commit();
        }
    }
}
