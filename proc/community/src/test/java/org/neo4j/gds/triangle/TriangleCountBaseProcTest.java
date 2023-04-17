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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.OnlyUndirectedTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class TriangleCountBaseProcTest<CONFIG extends TriangleCountBaseConfig> extends BaseProcTest
    implements AlgoBaseProcTest<IntersectingTriangleCount, CONFIG, TriangleCountResult>,
    OnlyUndirectedTest<IntersectingTriangleCount, CONFIG, TriangleCountResult> {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
                                           "(a:A)-[:T]->(b:A), " +
                                           "(b)-[:T]->(c:A), " +
                                           "(c)-[:T]->(a)";


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class,
            getProcedureClazz()
        );

        runQuery(formatWithLocale(
            "CALL gds.graph.project('%s', 'A', {T: { orientation: 'UNDIRECTED'}})",
            DEFAULT_GRAPH_NAME
        ));
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(
        TriangleCountResult result1, TriangleCountResult result2
    ) {
        // TODO: add checks for the HugeArrays
        assertEquals(result1.globalTriangles(), result2.globalTriangles());
    }

    @Override
    public RelationshipProjections relationshipProjections() {
        return RelationshipProjections.ALL_UNDIRECTED;
    }

    @Test
    void testMaxDegreeValidation() {
        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.empty().withNumber("maxDegree", 1L));

        applyOnProcedure(proc -> {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> proc.configParser().processInput(config.toMap())
            );

            String message = exception.getMessage();
            assertTrue(message.contains("maxDegree") && message.contains("greater than 1") );
        });
    }

    @Override
    public void loadGraph(String graphName){
        loadGraph(graphName, Orientation.UNDIRECTED);
    }
}
