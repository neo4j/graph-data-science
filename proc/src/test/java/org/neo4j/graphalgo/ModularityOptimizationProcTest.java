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

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.GraphCatalog;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;

class ModularityOptimizationProcTest extends BaseProcTest {

    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 1})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";

    private static final long[][] UNWEIGHTED_COMMUNITIES = {new long[]{0, 1, 2, 4}, new long[]{3, 5}};
    private static final long[][] WEIGHTED_COMMUNITIES = {new long[]{0, 4, 5}, new long[]{1, 2, 3}};
    private static final long[][] SEEDED_COMMUNITIES = {new long[]{0, 1}, new long[]{2, 3, 4, 5}};


    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(ModularityOptimizationProc.class, GraphLoadProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void writingOnImplicitlyLoadedGraph() {
        String query = "CALL algo.beta.modularityOptimization.write(" +
                       "    null, null, {" +
                       "        write: true, writeProperty: 'community', direction: 'BOTH'" +
                       "    }" +
                       ")";

        runQuery(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            assertEquals(0.12244, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber("communityCount").longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });

        assertWriteResult(UNWEIGHTED_COMMUNITIES);
    }

    @Test
    @Disabled
    void weightedWritingOnImplicitlyLoadedGraph() {
        String query = "CALL algo.beta.modularityOptimization.write(" +
                       "    null, null, {" +
                       "        write: true, writeProperty: 'community', direction: 'BOTH', relationshipProperties: 'weight'" +
                       "    }" +
                       ")";

        runQuery(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            assertEquals(0.4985, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber("communityCount").longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });

        assertWriteResult(WEIGHTED_COMMUNITIES);
    }

    @Test
    void streamingOnImplicitlyLoadedGraph() {
        String query = "CALL algo.beta.modularityOptimization.stream(" +
                       "    null, null, {" +
                       "        direction: 'BOTH'" +
                       "    }" +
                       ") YIELD nodeId, community";

        long[] communities = new long[6];
        runQuery(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities[(int) nodeId] = row.getNumber("community").longValue();
        });

        assertCommunities(communities, UNWEIGHTED_COMMUNITIES);
    }

    @Test
    @Disabled
    void weightedStreamingOnImplicitlyLoadedGraph() {
        String query = "CALL algo.beta.modularityOptimization.stream(" +
                       "    null, null, {" +
                       "        direction: 'BOTH', relationshipProperties: 'weight'" +
                       "    }" +
                       ") YIELD nodeId, community";

        long[] communities = new long[6];
        runQuery(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities[(int) nodeId] = row.getNumber("community").longValue();
        });

        assertCommunities(communities, WEIGHTED_COMMUNITIES);
    }

    @Test
    void streamingWithSeeding() {
        String query = "CALL algo.beta.modularityOptimization.stream(" +
                       "    null, null, {" +
                       "        direction: 'BOTH', seedProperty: 'seed1'" +
                       "    }" +
                       ") YIELD nodeId, community";

        long[] communities = new long[6];
        runQuery(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities[(int) nodeId] = row.getNumber("community").longValue();
        });

        assertCommunities(communities, SEEDED_COMMUNITIES);
        assertTrue(communities[0] == 0 && communities[2] == 2);
    }

    @Test
    void writingWithSeeding() {
        String query = "CALL algo.beta.modularityOptimization.write(" +
                       "    null, null, {" +
                       "        write: true, writeProperty: 'community', direction: 'BOTH', seedProperty: 'seed1'" +
                       "    }" +
                       ")";
        runQuery(query);

        long[] communities = new long[6];
        MutableInt i = new MutableInt(0);
        runQuery("MATCH (n) RETURN n.community as community", (row) -> {
            communities[i.getAndIncrement()] = row.getNumber("community").longValue();
        });
    }

    @Test
    void tolerance() {
        String query = "CALL algo.beta.modularityOptimization.write(" +
                       "    null, null, {" +
                       "        write: false, direction: 'BOTH', tolerance: 1" +
                       "    }" +
                       ") YIELD didConverge, ranIterations";

        runQuery(query, (row) -> {
            assertTrue(row.getBoolean("didConverge"));
            assertEquals(1, row.getNumber("ranIterations").longValue());
        });
    }

    @Test
    void setIterations() {
        String query = "CALL algo.beta.modularityOptimization.write(" +
                       "    null, null, {" +
                       "        write: false, direction: 'BOTH', iterations: 1" +
                       "    }" +
                       ") YIELD didConverge, ranIterations";

        runQuery(query, (row) -> {
            assertFalse(row.getBoolean("didConverge"));
            assertEquals(1, row.getNumber("ranIterations").longValue());
        });
    }

    @Test
    void computeMemrec() {
        String query = "CALL algo.beta.modularityOptimization.memrec(" +
                       "    null, null" +
                       ") YIELD requiredMemory, treeView, bytesMin, bytesMax";

        runQuery(query, (row) -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);
        });
    }

    private void assertWriteResult(long[]... expectedCommunities) {
        Map<String, Object> nameMapping = MapUtil.map(
            "a", 0,
            "b", 1,
            "c", 2,
            "d", 3,
            "e", 4,
            "f", 5
        );
        long[] actualCommunities = new long[6];
        runQuery("MATCH (n) RETURN n.name as name, n.community as community", (row) -> {
            long community = row.getNumber("community").longValue();
            String name = row.getString("name");
            actualCommunities[(int) nameMapping.get(name)] = community;
        });

        assertCommunities(actualCommunities, expectedCommunities);
    }
}
