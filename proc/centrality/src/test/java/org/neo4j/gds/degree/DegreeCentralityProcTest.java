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
package org.neo4j.gds.degree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class DegreeCentralityProcTest<CONFIG extends DegreeCentralityConfig>
    extends BaseProcTest
    implements
    AlgoBaseProcTest<DegreeCentrality, CONFIG, DegreeCentrality.DegreeFunction>,
    MemoryEstimateTest<DegreeCentrality, CONFIG, DegreeCentrality.DegreeFunction>
{

    @TestFactory
    final Stream<DynamicTest> configTests() {
        return modeSpecificConfigTests();
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    static final String DEFAULT_RESULT_PROPERTY = "degreeScore";
    static final String GRAPH_NAME = "dcGraph";

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +
        ", (h:Label1)" +
        ", (i:Label1)" +
        ", (j:Label1)" +

        ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +

        ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

        ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 2.0}]->(e)";

    @Inject
    protected IdFunction idFunction;

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

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @Override
    public void assertResultEquals(
        DegreeCentrality.DegreeFunction result1, DegreeCentrality.DegreeFunction result2
    ) {
        var graph = GraphStoreCatalog.get(getUsername(), databaseId(), GRAPH_NAME).graphStore().getUnion();
        graph.forEachNode(nodeId -> {
            assertEquals(result1.get(nodeId), result2.get(nodeId));
            return true;
        });
    }
}
