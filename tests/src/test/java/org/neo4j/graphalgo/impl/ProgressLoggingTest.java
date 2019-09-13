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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GridBuilder;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class ProgressLoggingTest {

    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static GraphDatabaseAPI db;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.<Object[]>asList(
//                new Object[]{LightGraphFactory.class, "LightGraphFactory"}, // doesn't log yet
//                new Object[]{GraphViewFactory.class, "GraphViewFactory"}, // doesn't log yet
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    @BeforeClass
    public static void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms"))) {
            final GridBuilder gridBuilder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newGridBuilder()
                    .createGrid(100, 10)
                    .forEachRelInTx(rel -> {
                        rel.setProperty(PROPERTY, Math.random() * 5); // (0-5)
                    });
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    public ProgressLoggingTest(Class<? extends GraphFactory> graphImpl, String nameIgnored) {
        this.graphImpl = graphImpl;
        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                .load(graphImpl);
    }

    @Test
    public void testLoad() throws Exception {

        final StringWriter buffer = new StringWriter();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLog(testLogger(buffer))
                    .withExecutorService(Pools.DEFAULT)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                    .load(graphImpl);
        }

        System.out.println(buffer);

        final String output = buffer.toString();

        assertTrue(output.length() > 0);
        assertTrue(output.contains(GraphFactory.TASK_LOADING));
    }

    @Test
    public void testWrite() throws Exception {

        final StringWriter buffer = new StringWriter();

        final int[] ints = new int[(int) graph.nodeCount()];
        Arrays.fill(ints, -1);

        Exporter.of(db, graph)
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
        assertTrue(output.contains(Exporter.TASK_EXPORT));
    }

    public static Log testLogger(StringWriter writer) {
        return FormattedLog
                .withLogLevel(Level.DEBUG)
                .withCategory("Test")
                .toPrintWriter(new PrintWriter(writer));
    }
}
