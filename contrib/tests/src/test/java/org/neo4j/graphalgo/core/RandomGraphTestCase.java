/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public abstract class RandomGraphTestCase {
    private static boolean hasFailures = false;

    protected static GraphDatabaseAPI db;

    static final int NODE_COUNT = 100;

    @Rule
    public TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(
                final Throwable e,
                final Description description) {
            hasFailures = true;
        }
    };

    private static final String RANDOM_GRAPH_TPL =
            "FOREACH (x IN range(1, %d) | CREATE (:Label)) " +
                    "WITH 0.1 AS p " +
                    "MATCH (n1),(n2) WITH n1,n2,p LIMIT 1000 WHERE rand() < p " +
                    "CREATE (n1)-[:TYPE {weight:ceil(10*rand())/10}]->(n2)";

    private static final String RANDOM_LABELS =
            "MATCH (n) WHERE rand() < 0.5 SET n:Label2";

    @BeforeClass
    public static void setupGraph() {
        db = buildGraph(NODE_COUNT);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (hasFailures) {
            try {
                PrintWriter pw = new PrintWriter(System.out);
                pw.println("Generated graph to reproduce any errors:");
                pw.println();
                CypherExporter.export(pw, db);
            } catch (Exception e) {
                System.err.println("Error exporting graph "+e.getMessage());
            }
        }
        if (db!=null) db.shutdown();
    }

    static GraphDatabaseAPI buildGraph(int nodeCount) {
        String createGraph = String.format(RANDOM_GRAPH_TPL, nodeCount);
        List<String> cyphers = Arrays.asList(createGraph, RANDOM_LABELS);

        final GraphDatabaseService db = TestDatabaseCreator.createTestDatabase();
        for (String cypher : cyphers) {
            try (Transaction tx = db.beginTx()) {
                 db.execute(cypher).close();
                tx.success();
            } catch (Exception e) {
                markFailure();
                throw e;
            }
        }
        return (GraphDatabaseAPI) db;
    }

    static void markFailure() {
        hasFailures = true;
    }
}
