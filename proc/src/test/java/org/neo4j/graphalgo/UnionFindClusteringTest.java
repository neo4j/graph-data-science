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
package org.neo4j.graphalgo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class UnionFindClusteringTest {

    private static final String CLUSTERN = "\n" +
                                           "CREATE (u1:User:Multi {expectedId:$id1})\n" +
                                           "CREATE (u2:User:Multi {expectedId:$id1})\n" +
                                           "CREATE (u3:User:Multi {expectedId:$id1})\n" +
                                           "CREATE (u4:User:Multi {expectedId:$id2})\n" +
                                           "CREATE (u5:User:Multi {expectedId:$id2})\n" +
                                           "CREATE (u6:User:Multi {expectedId:$id2})\n" +
                                           "CREATE (u7:User:Multi {expectedId:$id3})\n" +
                                           "CREATE (u8:User:Multi {expectedId:$id3})\n" +
                                           "CREATE (u9:User:Multi {expectedId:$id3})\n" +
                                           "CREATE (u10:User:Multi {expectedId:$id4})\n" +
                                           "CREATE (u11:User:Multi {expectedId:$id4})\n" +
                                           "CREATE (u12:User:Multi {expectedId:$id4})\n" +
                                           "CREATE (u13:User:Multi {expectedId:$id5})\n" +
                                           "CREATE (u14:User:Multi {expectedId:$id5})\n" +
                                           "CREATE (u15:User:Multi {expectedId:$id5})\n" +
                                           "CREATE (u16:User:Multi {expectedId:$id6})\n" +
                                           "CREATE (u17:User:Multi {expectedId:$id6})\n" +
                                           "CREATE (u18:User:Multi {expectedId:$id6})\n" +
                                           "CREATE (u19:User:Multi {expectedId:$id7})\n" +
                                           "CREATE (u20:User:Multi {expectedId:$id7})\n" +
                                           "CREATE (u21:User:Multi {expectedId:$id7})\n" +
                                           "CREATE (u22:User:Multi {expectedId:$id8})\n" +
                                           "CREATE (u23:User:Multi {expectedId:$id8})\n" +
                                           "CREATE (u24:User:Multi {expectedId:$id8})\n" +
                                           "CREATE (u25:User:Multi {expectedId:$id9})\n" +
                                           "CREATE (u26:User:Multi {expectedId:$id9})\n" +
                                           "CREATE (u27:User:Multi {expectedId:$id9})\n" +
                                           "CREATE (u28:User:Multi {expectedId:$id10})\n" +
                                           "CREATE (u29:User:Multi {expectedId:$id10})\n" +
                                           "CREATE (u30:User:Multi {expectedId:$id10})\n" +
                                           "CREATE (n1:Other:Multi {expectedId:$id1})\n" +
                                           "CREATE (n2:Other:Multi {expectedId:$id2})\n" +
                                           "CREATE (n3:Other:Multi {expectedId:$id3})\n" +
                                           "CREATE (n4:Other:Multi {expectedId:$id4})\n" +
                                           "CREATE (n5:Other:Multi {expectedId:$id5})\n" +
                                           "CREATE (n6:Other:Multi {expectedId:$id6})\n" +
                                           "CREATE (n7:Other:Multi {expectedId:$id7})\n" +
                                           "CREATE (n8:Other:Multi {expectedId:$id8})\n" +
                                           "CREATE (n9:Other:Multi {expectedId:$id9})\n" +
                                           "CREATE (n10:Other:Multi {expectedId:$id10})\n" +
                                           "CREATE\n" +
                                           "  (u1)-[:OBSERVED_WITH]->(n1)<-[:OBSERVED_WITH]-(u2),\n" +
                                           "  (u1)-[:OBSERVED_WITH]->(n1)<-[:OBSERVED_WITH]-(u3),\n" +
                                           "  (u4)-[:OBSERVED_WITH]->(n2)<-[:OBSERVED_WITH]-(u5),\n" +
                                           "  (u4)-[:OBSERVED_WITH]->(n2)<-[:OBSERVED_WITH]-(u6),\n" +
                                           "  (u7)-[:OBSERVED_WITH]->(n3)<-[:OBSERVED_WITH]-(u8),\n" +
                                           "  (u7)-[:OBSERVED_WITH]->(n3)<-[:OBSERVED_WITH]-(u9),\n" +
                                           "  (u10)-[:OBSERVED_WITH]->(n4)<-[:OBSERVED_WITH]-(u11),\n" +
                                           "  (u10)-[:OBSERVED_WITH]->(n4)<-[:OBSERVED_WITH]-(u12),\n" +
                                           "  (u13)-[:OBSERVED_WITH]->(n5)<-[:OBSERVED_WITH]-(u14),\n" +
                                           "  (u13)-[:OBSERVED_WITH]->(n5)<-[:OBSERVED_WITH]-(u15),\n" +
                                           "  (u16)-[:OBSERVED_WITH]->(n6)<-[:OBSERVED_WITH]-(u17),\n" +
                                           "  (u16)-[:OBSERVED_WITH]->(n6)<-[:OBSERVED_WITH]-(u18),\n" +
                                           "  (u19)-[:OBSERVED_WITH]->(n7)<-[:OBSERVED_WITH]-(u20),\n" +
                                           "  (u19)-[:OBSERVED_WITH]->(n7)<-[:OBSERVED_WITH]-(u21),\n" +
                                           "  (u22)-[:OBSERVED_WITH]->(n8)<-[:OBSERVED_WITH]-(u23),\n" +
                                           "  (u22)-[:OBSERVED_WITH]->(n8)<-[:OBSERVED_WITH]-(u24),\n" +
                                           "  (u25)-[:OBSERVED_WITH]->(n9)<-[:OBSERVED_WITH]-(u26),\n" +
                                           "  (u25)-[:OBSERVED_WITH]->(n9)<-[:OBSERVED_WITH]-(u27),\n" +
                                           "  (u28)-[:OBSERVED_WITH]->(n10)<-[:OBSERVED_WITH]-(u29),\n" +
                                           "  (u28)-[:OBSERVED_WITH]->(n10)<-[:OBSERVED_WITH]-(u30);\n";

    @Parameterized.Parameters(name = "graph={0}, incremental={1}")
    public static Collection<Object[]> data() {
        return Arrays.<Object[]>asList(
                new Object[]{"Heavy", true},
                new Object[]{"Heavy", false},
                new Object[]{"Huge", true},
                new Object[]{"Huge", false}
        );
    }

    @Parameterized.Parameter(0)
    public String graphName;

    @Parameterized.Parameter(1)
    public boolean incremental;

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {
        for (int i = 0; i < 1; i++) {
            final int s = i;
            Map<String, Object> params = IntStream.rangeClosed(1, 10)
                    .mapToObj(x -> new AbstractMap.SimpleImmutableEntry<>("id" + x, s * 10 + x))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            DB.execute(CLUSTERN, params);
        }
        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(UnionFindProc.class);
    }

    @Test
    public void test() {
        String communityProperty = incremental
                ? ",communityProperty:'expectedId'"
                : "";

        String queryTemplate =
                "CALL algo.unionFind(\n" +
                "  'Multi',\n" +
                "  'OBSERVED_WITH', {\n" +
                "    graph:'%s',\n" +
                "    write:true,\n" +
                "    writeProperty:'actualId',\n" +
                "    concurrency:1\n" +
                "          %s\n" +
                "  })";
        DB
                .execute(String.format(queryTemplate, graphName, communityProperty))
                .close();

        DB
                .execute("MATCH (n) RETURN n.expectedId AS expectedId, collect(DISTINCT n.actualId) AS actualClusters")
                .accept((row) -> {
                    Collection<?> clusters = (Collection<?>) row.get("actualClusters");
                    String errorMessage = String.format(
                            "Multiple clusters FOR expected cluster %d: %s",
                            row.getNumber("expectedId").longValue(),
                            clusters
                    );
                    assertEquals(errorMessage, 1L, clusters.size());
                    return true;
                });
    }
}
