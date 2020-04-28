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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.neo4j.graphalgo.BaseTest;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ExtendWith(RandomGraphTestCase.TestWatcherExtension.class)
public abstract class RandomGraphTestCase extends BaseTest {

    private static boolean hasFailures = false;

    static final int NODE_COUNT = 100;

    static class TestWatcherExtension implements TestWatcher {

        TestWatcherExtension() {}

        @Override
        public void testFailed(
                ExtensionContext context,
                final Throwable e) {
            hasFailures = true;
        }
    }

    private static final String RANDOM_GRAPH_TPL =
            "FOREACH (x IN range(1, %d) | CREATE (:Label)) " +
            "WITH 0.1 AS p " +
            "MATCH (n1),(n2) WITH n1,n2,p LIMIT 1000 WHERE rand() < p " +
            "CREATE (n1)-[:TYPE {weight:ceil(10*rand())/10}]->(n2)";

    private static final String RANDOM_LABELS =
            "MATCH (n) WHERE rand() < 0.5 SET n:Label2";

    @BeforeEach
    void setupGraph() {
        buildGraph(NODE_COUNT);
    }

    @AfterEach
    void shutdownGraph() {
        if (hasFailures) {
            try {
                PrintWriter pw = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
                pw.println("Generated graph to reproduce any errors:");
                pw.println();
                CypherExporter.export(pw, db);
            } catch (Exception e) {
                System.err.println("Error exporting graph "+e.getMessage());
            }
        }
    }

    void buildGraph(int nodeCount) {
        String createGraph = formatWithLocale(RANDOM_GRAPH_TPL, nodeCount);
        List<String> cyphers = Arrays.asList(createGraph, RANDOM_LABELS);

        for (String cypher : cyphers) {
            try {
                runQuery(cypher);
            } catch (Exception e) {
                markFailure();
                throw e;
            }
        }
    }

    static void markFailure() {
        hasFailures = true;
    }
}
