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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.L2Norm;
import org.neo4j.gds.embeddings.graphsage.proc.GraphSageStreamProc;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteProc;
import org.neo4j.gds.embeddings.randomprojections.RandomProjectionWriteProc;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class EmbeddingsIntegrationTest extends BaseProcTest {

    private static final String TEST_GRAPH = "testGraph";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node { nodeId: 0 })" +
        ", (b:Node { nodeId: 1 })" +
        ", (c:Node { nodeId: 2 })" +
        ", (d:Node { nodeId: 3 })" +
        ", (e:Node { nodeId: 4 })" +
        ", (f:Node { nodeId: 5 })" +
        ", (g:Node { nodeId: 6 })" +
        ", (h:Node { nodeId: 7 })" +
        ", (i:Node { nodeId: 8 })" +
        ", (j:Node { nodeId: 9 })" +
        ", (k:Node { nodeId: 10 })" +
        ", (l:Node { nodeId: 11 })" +
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(g)" +
        ", (h)-[:TYPE]->(i)" +
        ", (i)-[:TYPE]->(k)" +
        ", (i)-[:TYPE]->(l)" +
        ", (j)-[:TYPE]->(k)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(
            GraphCreateProc.class,
            GraphSageStreamProc.class,
            RandomProjectionWriteProc.class,
            Node2VecWriteProc.class
        );

        runQuery(GdsCypher
            .call()
            .withAnyLabel()
            .withNodeProperty("nodeId")
            .withRelationshipType("TYPE")
            .graphCreate(TEST_GRAPH)
            .yields());
    }

    @AfterEach
    void shutdown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void runPipeline() {
        // run algorithms in write mode

        int node2vecEmbeddingSize = 3;
        String node2vecQuery = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH)
            .algo("gds.alpha.node2vec")
            .writeMode()
            .addParameter("embeddingSize", node2vecEmbeddingSize)
            .addParameter("writeProperty", "node2vec")
            .yields();

        runQuery(node2vecQuery);

        int rpEmbeddingSize = 3;
        String rpQuery = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH)
            .algo("gds.alpha.randomProjection")
            .writeMode()
            .addParameter("embeddingSize", rpEmbeddingSize)
            .addParameter("maxIterations", 2)
            .addParameter("iterationWeights", List.of(1D, 1D))
            .addParameter("writeProperty", "rp")
            .yields();

        runQuery(rpQuery);

        String newCreateQuery = GdsCypher
            .call()
            .withNodeLabel("MATCH (n) " +
                           "RETURN " +
                           "  id(n) AS id, " +
                           "  n.rp[0] AS rp0," +
                           "  n.rp[1] AS rp1," +
                           "  n.rp[2] AS rp2," +
                           "  n.node2vec[0] AS node2vec0," +
                           "  n.node2vec[1] AS node2vec1," +
                           "  n.node2vec[2] AS node2vec2")
            .withRelationshipType("MATCH (n)-->(m) " +
                                  "RETURN" +
                                  "  id(n) AS source," +
                                  "  id(m) AS target")
            .graphCreateCypher("newGraph")
            .yields();
        runQuery(newCreateQuery);

        // run GraphSage in stream mode
        int embeddingSize = 64;
        String graphSageQuery = GdsCypher
            .call()
            .explicitCreation("newGraph")
            .algo("gds.alpha.graphSage")
            .streamMode()
            .addParameter("nodePropertyNames", List.of("rp0", "rp1", "rp2", "node2vec0", "node2vec1", "node2vec2"))
            .addParameter("embeddingSize", embeddingSize)
            .yields();

        runQueryWithRowConsumer(graphSageQuery, row -> {
            Collection<Double> embeddings = (Collection<Double>) row.get("embeddings");
            assertEquals(embeddings.size(), embeddingSize);

            double[] values = embeddings.stream()
                .mapToDouble(Double::doubleValue)
                .toArray();
            assertNotEquals(0D, L2Norm.l2(Tensor.vector(values)));
        });
    }
}
