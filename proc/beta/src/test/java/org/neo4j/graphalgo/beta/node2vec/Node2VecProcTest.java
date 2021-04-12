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
package org.neo4j.graphalgo.beta.node2vec;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Vector;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.HeapControlTest;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.RelationshipWeightConfigTest;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class Node2VecProcTest<CONFIG extends Node2VecBaseConfig> extends
    BaseProcTest implements AlgoBaseProcTest<Node2Vec, CONFIG, HugeObjectArray<Vector>>,
    MemoryEstimateTest<Node2Vec, CONFIG, HugeObjectArray<Vector>>,
    HeapControlTest<Node2Vec, CONFIG, HugeObjectArray<Vector>>,
    RelationshipWeightConfigTest<Node2Vec, CONFIG, HugeObjectArray<Vector>>{

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
            GraphCreateProc.class
        );
    }

    public GraphDatabaseAPI graphDb() {
        return db;
    }

    public void assertResultEquals(HugeObjectArray<Vector> result1, HugeObjectArray<Vector> result2) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.get(0).data().length, result2.get(0).data().length);
    }

}
