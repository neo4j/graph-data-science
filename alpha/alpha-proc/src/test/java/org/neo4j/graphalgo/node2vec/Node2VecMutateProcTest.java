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
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.functions.NodePropertyFunc;
import org.neo4j.graphdb.Result;

import java.util.Map;

class Node2VecMutateProcTest extends BaseProcTest {

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
        registerProcedures(GraphCreateProc.class, Node2VecMutateProc.class);
        registerFunctions(NodePropertyFunc.class);
    }

    @Test
    void embeddingsShouldHaveTheConfiguredDimension() {
        long dimensions = 42;
        var graphName = "my-graph";
        var mutateProperty = "vector";


        runQuery(GdsCypher.call().loadEverything().graphCreate(graphName).yields());

        var query = GdsCypher.call()
            .explicitCreation(graphName)
            .algo("gds.alpha.node2vec")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty)
            .addParameter("dimensions", dimensions)
            .yields();
        runQuery(query);

        var result = runQuery(
            "MATCH (n) RETURN gds.util.nodeProperty($graphName, id(n), $propertyKey) AS vector",
            Map.of("graphName", graphName, "propertyKey", mutateProperty),
            Result::resultAsString
        );
        System.out.println(result);

    }
}
