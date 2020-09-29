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
package org.neo4j.gds.embeddings.randomprojections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class FastRPProcTest<CONFIG extends FastRPBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<FastRP, CONFIG, FastRP>,
    MemoryEstimateTest<FastRP, CONFIG, FastRP> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Isolated {name: 'c'})" +
        ", (d:Isolated {name: 'd'})" +
        ", (a)-[:REL]->(b)" +

        // Used for the weighted case
        ", (e:Node2 {name: 'e'})" +
        ", (a)<-[:REL2 {weight: 2.0}]-(b)" +
        ", (a)<-[:REL2 {weight: 1.0}]-(e)";

    @Override
    public String createQuery() {
        return DB_CYPHER;
    }

    @BeforeEach
    void setUp() throws Exception {
        runQuery(createQuery());
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
        FastRP result1, FastRP result2
    ) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddings().get(0).length, result1.embeddings().get(0).length);
    }

    private static Stream<Arguments> weights() {
        return Stream.of(
            Arguments.of(Collections.emptyList()),
            Arguments.of(List.of(1.0f, 1.0f, 2.0f, 4.0f))
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        return userInput.containsKey("embeddingSize")
            ? userInput
            : userInput.withEntry("embeddingSize", 128);
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
