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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.OrientationCombinationTest;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class BetweennessCentralityProcTest<CONFIG extends BetweennessCentralityBaseConfig>
    extends BaseProcTest
    implements
    AlgoBaseProcTest<BetweennessCentrality, CONFIG, HugeAtomicDoubleArray>,
    MemoryEstimateTest<BetweennessCentrality, CONFIG, HugeAtomicDoubleArray>,
    OrientationCombinationTest<BetweennessCentrality, CONFIG, HugeAtomicDoubleArray> {

    static final String DEFAULT_RESULT_PROPERTY = "centrality";
    protected static final String BC_GRAPH_NAME = "bcGraph";

    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE" +
                                       "  (a:Node {name: 'a'})" +
                                       ", (b:Node {name: 'b'})" +
                                       ", (c:Node {name: 'c'})" +
                                       ", (d:Node {name: 'd'})" +
                                       ", (e:Node {name: 'e'})" +
                                       ", (a)-[:REL]->(b)" +
                                       ", (b)-[:REL]->(c)" +
                                       ", (c)-[:REL]->(d)" +
                                       ", (d)-[:REL]->(e)";

    List<Map<String, Object>> expected;

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );

        expected = List.of(
            Map.of("nodeId", idFunction.of("a"), "score", 0.0),
            Map.of("nodeId", idFunction.of("b"), "score", 3.0),
            Map.of("nodeId", idFunction.of("c"), "score", 4.0),
            Map.of("nodeId", idFunction.of("d"), "score", 3.0),
            Map.of("nodeId", idFunction.of("e"), "score", 0.0)
        );

        String loadQuery = GdsCypher.call(BC_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType(
                "REL",
                RelationshipProjection.of(
                    "REL",
                    Orientation.UNDIRECTED,
                    Aggregation.DEFAULT
                )
            )
            .yields();

        runQuery(loadQuery);
    }

    @Override
    public void assertResultEquals(HugeAtomicDoubleArray result1, HugeAtomicDoubleArray result2) {
        assertEquals(result1.size(), result2.size());

        for (long nodeId = 0; nodeId < result1.size(); nodeId++) {
            assertEquals(result1.get(nodeId), result2.get(nodeId));
        }
    }

}
