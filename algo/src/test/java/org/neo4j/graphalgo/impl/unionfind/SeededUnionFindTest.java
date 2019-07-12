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
package org.neo4j.graphalgo.impl.unionfind;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.IntLongMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SeededUnionFindTest {

    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");
    private static final String COMMUNITY_PROPERTY = "community";

    private static final int SETS_COUNT = 16;
    private static final int SET_SIZE = 10;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{HugeGraphFactory.class, "Huge"}
                // TODO GraphView graph does not support node properties
                // new Object[]{GraphViewFactory.class, "Kernel"}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        int[] setSizes = new int[SETS_COUNT];
        Arrays.fill(setSizes, SET_SIZE);
        createTestGraph(setSizes);
    }

    private static void createTestGraph(int... setSizes) {
        DB.executeAndCommit(db -> {
            for (int i = 0; i < setSizes.length; i = i + 2) {
                long sourceId = createLine(db, setSizes[i]);
                long targetId = createLine(db, setSizes[i + 1]);

                createConnection(db, sourceId, targetId);
            }
        });
    }

    private static Long createLine(GraphDatabaseService db, int setSize) {
        Node temp = db.createNode();
        for (int i = 1; i < setSize; i++) {
            Node t = db.createNode();

            int communityId = (int) t.getId() / setSize * 2 + 1;
            temp.setProperty(COMMUNITY_PROPERTY, communityId);

            temp.createRelationshipTo(t, RELATIONSHIP_TYPE);
            temp = t;
        }

        int communityId = (int) temp.getId() / setSize * 2 + 1;
        temp.setProperty(COMMUNITY_PROPERTY, communityId);
        return temp.getId();
    }

    private static void createConnection(GraphDatabaseService db, long sourceId, long targetId) {
        final Node source = db.getNodeById(sourceId);
        final Node target = db.getNodeById(targetId);

        source.createRelationshipTo(target, RELATIONSHIP_TYPE);
    }

    private final Graph graph;
    private final UnionFind.Config config;

    public SeededUnionFindTest(Class<? extends GraphFactory> graphImpl, String name) {
        graph = new GraphLoader(DB)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withOptionalNodeProperties(
                        PropertyMapping.of(COMMUNITY_PROPERTY, COMMUNITY_PROPERTY, -1L)
                )
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(graphImpl);

        config = new UnionFind.Config(
                graph.nodeProperties(COMMUNITY_PROPERTY),
                Double.NaN,
                false
        );
    }

    @Test
    public void testSeq() {
        test(UnionFindType.SEQ);
    }

    @Test
    public void testQueue() {
        test(UnionFindType.QUEUE);
    }

    @Test
    public void testForkJoin() {
        test(UnionFindType.FORK_JOIN);
    }

    @Test
    public void testFJMerge() {
        test(UnionFindType.FJ_MERGE);
    }

    private void test(UnionFindType uf) {
        DisjointSetStruct result = run(uf);

        Assert.assertEquals(SETS_COUNT / 2, result.getSetCount());
        IntLongMap setRegions = new IntLongHashMap();

        graph.forEachNode((nodeId) -> {
            int expectedSetRegion = ((int) nodeId / (2 * SET_SIZE) * 2) * 2 + 1;
            final long setId = result.setIdOf(nodeId);
            int setRegion = (int) setId;
            assertEquals(
                    "Node " + nodeId + " in unexpected set: " + setId,
                    expectedSetRegion,
                    setRegion);

            long regionSetId = setRegions.getOrDefault(setRegion, -1);
            if (regionSetId == -1) {
                setRegions.put(setRegion, setId);
            } else {
                assertEquals(
                        "Inconsistent set for node " + nodeId + ", is " + setId + " but should be " + regionSetId,
                        regionSetId,
                        setId);
            }
            return true;
        });
    }

    private DisjointSetStruct run(final UnionFindType uf) {
        return UnionFindHelper.run(
                uf,
                graph,
                Pools.DEFAULT,
                SET_SIZE / Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT_CONCURRENCY,
                config,
                AllocationTracker.EMPTY);
    }
}
