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
package org.neo4j.graphalgo.node2vec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.node2vec.ImmutableNode2VecConfig;
import org.neo4j.graphalgo.impl.node2vec.Node2Vec;
import org.neo4j.graphalgo.impl.node2vec.Node2VecConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Node2VecStreamProcTest extends BaseProcTest implements AlgoBaseProcTest<Node2Vec, Node2VecConfig, Node2Vec> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (a)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)";

    @BeforeEach
    void setUp() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(getProcedureClazz());
    }

    @Test
    void embeddingsShouldHaveTheConfiguredDimension() {
        int dimensions = 42;
        var query = GdsCypher.call()
            .loadEverything()
            .algo("gds.alpha.node2vec")
            .streamMode()
            .addParameter("dimensions", 42)
            .yields();

        runQueryWithRowConsumer(query, row -> assertEquals(dimensions, ((List<Double>) row.get("vector")).size()));
    }

    @Override
    public Class<? extends AlgoBaseProc<Node2Vec, Node2Vec, Node2VecConfig>> getProcedureClazz() {
        return Node2VecStreamProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public Node2VecConfig createConfig(CypherMapWrapper userInput) {
        return Node2VecConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
    }

    @Override
    public void assertResultEquals(Node2Vec result1, Node2Vec result2) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddingForNode(0).length, result2.embeddingForNode(0).length);
    }
}
