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
package org.neo4j.gds.beta.fastrp;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.Orientation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class FastRPExtendedProcTest<CONFIG extends FastRPExtendedBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<FastRP, CONFIG, FastRP.FastRPResult>,
    MemoryEstimateTest<FastRP, CONFIG, FastRP.FastRPResult> {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a', f1: 0.4, f2: 1.3})" +
        ", (b:Node {name: 'b', f1: 2.1, f2: 0.5})" +
        ", (c:Isolated {name: 'c'})" +
        ", (d:Isolated {name: 'd'})" +
        ", (a)-[:REL]->(b)" +

        // Used for the weighted case
        ", (e:Node2 {name: 'e'})" +
        ", (a)<-[:REL2 {weight: 2.0}]-(b)" +
        ", (a)<-[:REL2 {weight: 1.0}]-(e)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class
        );
    }

    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(
        FastRP.FastRPResult result1, FastRP.FastRPResult result2
    ) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddings().get(0).length, result1.embeddings().get(0).length);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        return userInput.containsKey("embeddingDimension")
            ? userInput
            : userInput.withEntry("embeddingDimension", 128);
    }

    @Override
    public void loadGraph(String graphName) {
        String graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .graphCreate(graphName)
            .yields();
        runQuery(graphCreateQuery);
    }
}
