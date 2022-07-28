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
package org.neo4j.gds.beta.node2vec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class Node2VecProcTest<CONFIG extends Node2VecBaseConfig> extends
    BaseProcTest implements AlgoBaseProcTest<Node2Vec, CONFIG, Node2VecModel.Result>,
    MemoryEstimateTest<Node2Vec, CONFIG, Node2VecModel.Result> {

    @TestFactory
    final Stream<DynamicTest> configTests() {
        return modeSpecificConfigTests();
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
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
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class
        );
    }

    public GraphDatabaseService graphDb() {
        return db;
    }

    public void assertResultEquals(Node2VecModel.Result result1, Node2VecModel.Result result2) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddings().get(0).data().length, result2.embeddings().get(0).data().length);
    }

}
