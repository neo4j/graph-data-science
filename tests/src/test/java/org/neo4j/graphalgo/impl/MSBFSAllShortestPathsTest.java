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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * 1
 * (0)->(1)
 * 1 | 1 | 1
 * v   v
 * (2)->(3)
 * 1 | 1 | 1
 * v   v
 * (4)->(5)
 * 1 | 1 | 1
 * v   v
 * (6)->(7)
 * 1 | 1  | 1
 * v    v
 * (8)->(9)
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class MSBFSAllShortestPathsTest {

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static final int width = 2, height = 5;

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private Graph graph;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "View"}
        );
    }

    @BeforeClass
    public static void setup() throws Exception {
        try (ProgressTimer ignored = ProgressTimer.start(t -> System.out.println(
                "setup took " + t + "ms"))) {
            GraphBuilder.create(DB)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newGridBuilder()
                    .createGrid(width, height);
        }
    }

    public MSBFSAllShortestPathsTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {

        graph = new GraphLoader(DB)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .load(graphImpl);
    }

    @Test
    public void testResults() throws Exception {
        testASP(new MSBFSAllShortestPaths(graph, AllocationTracker.EMPTY, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT, Direction.OUTGOING));
    }

    private void testASP(final MSBFSASPAlgorithm<?> hugeMSBFSAllShortestPaths) {
        final ResultConsumer mock = mock(ResultConsumer.class);
        hugeMSBFSAllShortestPaths
                .resultStream()
                .forEach(r -> {
                    if (r.sourceNodeId > r.targetNodeId) {
                        fail("should not happen");
                    } else if (r.sourceNodeId == r.targetNodeId) {
                        fail("should not happen");
                    }
                    mock.test(r.sourceNodeId, r.targetNodeId, r.distance);
                });

        verify(mock, times(35)).test(anyLong(), anyLong(), anyDouble());
        verify(mock, times(1)).test(0, 9, 5.0);
    }

    interface ResultConsumer {

        void test(long source, long target, double distance);
    }
}
