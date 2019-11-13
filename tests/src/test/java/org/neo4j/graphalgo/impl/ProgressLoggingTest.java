/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.ExporterBuilder;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgressLoggingTest {

    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static GraphDatabaseAPI DB;

    private Graph graph;

    @BeforeAll
    static void setup() {
        DB = TestDatabaseCreator.createTestDatabase();

        GraphBuilder.create(DB)
                .setLabel(LABEL)
                .setRelationship(RELATIONSHIP)
                .newGridBuilder()
                .createGrid(100, 10)
                .forEachRelInTx(rel -> {
                    rel.setProperty(PROPERTY, Math.random() * 5); // (0-5)
                });
    }

    @AfterAll
    static void shutdown() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testLoad(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final StringWriter buffer = new StringWriter();

        graph = new GraphLoader(DB)
                .withLog(testLogger(buffer))
                .withExecutorService(Pools.DEFAULT)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .withRelationshipProperties(PropertyMapping.of(PROPERTY, 1.0))
                .load(graphFactory);

        final String output = buffer.toString();
        assertTrue(output.length() > 0);
        assertTrue(output.contains(GraphFactory.TASK_LOADING));
    }

    @AllGraphTypesWithoutCypherTest
    void testWrite(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final StringWriter buffer = new StringWriter();

        final int[] ints = new int[(int) graph.nodeCount()];
        Arrays.fill(ints, -1);

        NodePropertyExporter.of(DB, graph, TerminationFlag.RUNNING_TRUE)
                .withLog(testLogger(buffer))
                .build()
                .write(
                        "test",
                        ints,
                        Translators.INT_ARRAY_TRANSLATOR
                );

        System.out.println(buffer);

        final String output = buffer.toString();

        assertTrue(output.length() > 0);
        assertTrue(output.contains(ExporterBuilder.TASK_EXPORT));
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(DB)
                .withExecutorService(Pools.DEFAULT)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .withRelationshipProperties(PropertyMapping.of(PROPERTY, 1.0))
                .load(graphImpl);
    }

    private static Log testLogger(StringWriter writer) {
        return FormattedLog
                .withLogLevel(Level.DEBUG)
                .withCategory("Test")
                .toPrintWriter(new PrintWriter(writer));
    }
}
