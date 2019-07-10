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
package org.neo4j.graphalgo.impl.labelprop;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class NonStabilizingLabelPropagationTest {

    private static final String GRAPH =
            "CREATE " +
            " (a {label:1})" +
            ",(b {label:1})" +
            ",(c {label:1})" +
            ",(d {label:2})" +
            ",(e {label:2})" +
            ",(f {label:2})" +
            ",(g {label:3})" +
            ",(h {label:4})" +
            "CREATE " +
            " (g)-[:R]->(a)" +
            ",(a)-[:R]->(d)" +
            ",(d)-[:R]->(b)" +
            ",(b)-[:R]->(e)" +
            ",(e)-[:R]->(c)" +
            ",(c)-[:R]->(f)" +
            ",(f)-[:R]->(h)";

    @Parameterized.Parameters(name = "graph={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class},
                new Object[]{HeavyCypherGraphFactory.class},
                new Object[]{HugeGraphFactory.class}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        DB.execute(GRAPH).close();
    }

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private final Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public NonStabilizingLabelPropagationTest(Class<? extends GraphFactory> graphImpl) {
        this.graphImpl = graphImpl;
    }

    @Before
    public void setup() {
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT)
                .asUndirected(true)
                .withConcurrency(Pools.DEFAULT_CONCURRENCY);

        if (graphImpl == HeavyCypherGraphFactory.class) {
            graphLoader
                    .withLabel("MATCH (u) RETURN id(u) as id")
                    .withRelationshipType("MATCH (u1)-[rel]-(u2) \n" +
                                          "RETURN id(u1) AS source, id(u2) AS target")
                    .withName("cypher");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withRelationshipType("R")
                    .withName(graphImpl.getSimpleName());
        }
        graph = graphLoader.load(graphImpl);
    }

    // According to "Near linear time algorithm to detect community structures in large-scale networks"[1], for a graph of this shape
    // LabelPropagation will not converge unless the iteration is random. However, we don't seem to be affected by this.
    // [1]: https://arxiv.org/pdf/0709.2938.pdf, page 5
    @Test
    public void testLabelPropagationDoesStabilize() {
        LabelPropagation labelPropagation = new LabelPropagation(graph, graph, ParallelUtil.DEFAULT_BATCH_SIZE, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT, AllocationTracker.EMPTY);
        LabelPropagation compute = labelPropagation.compute(Direction.OUTGOING, 10);
        LabelPropagation.Labels result = compute.labels();
        assertTrue("Should converge", compute.didConverge());
    }


}
