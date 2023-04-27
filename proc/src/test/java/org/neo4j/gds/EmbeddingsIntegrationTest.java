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
package org.neo4j.gds;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageStreamProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageTrainProc;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.math.L2Norm.l2Norm;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Neo4jModelCatalogExtension
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
            GraphProjectProc.class,
            GraphSageTrainProc.class,
            GraphSageStreamProc.class,
            FastRPWriteProc.class,
            Node2VecWriteProc.class
        );

        runQuery(GdsCypher
            .call(TEST_GRAPH)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("nodeId")
            .withRelationshipType("TYPE")
            .yields());
    }

    @AfterEach
    void shutdown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void runPipeline() {
        // run algorithms in write mode

        int node2vecEmbeddingDimension = 3;
        String node2vecQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("embeddingDimension", node2vecEmbeddingDimension)
            .addParameter("writeProperty", "node2vec")
            .yields();

        runQuery(node2vecQuery);

        int rpEmbeddingDimension = 3;
        String rpQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("gds.fastRP")
            .writeMode()
            .addParameter("embeddingDimension", rpEmbeddingDimension)
            .addParameter("iterationWeights", List.of(1D, 1D))
            .addParameter("writeProperty", "rp")
            .yields();

        runQuery(rpQuery);

        String newCreateQuery = formatWithLocale(
            "CALL gds.graph.project.cypher('newGraph', '%s', '%s')",
            "MATCH (n) " +
            "RETURN " +
            "  id(n) AS id, " +
            "  n.rp[0] AS rp0," +
            "  n.rp[1] AS rp1," +
            "  n.rp[2] AS rp2," +
            "  n.node2vec[0] AS node2vec0," +
            "  n.node2vec[1] AS node2vec1," +
            "  n.node2vec[2] AS node2vec2",
            "MATCH (n)-->(m) " +
            "RETURN" +
            "  id(n) AS source," +
            "  id(m) AS target"
        );
        runQuery(newCreateQuery);

        // run GraphSage in stream mode
        int embeddingDimension = 64;
        String graphSageModel = "graphSageModel";
        String graphSageTrainQuery = GdsCypher
            .call("newGraph")
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("rp0", "rp1", "rp2", "node2vec0", "node2vec1", "node2vec2"))
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("modelName", graphSageModel)
            .yields();

        runQuery(graphSageTrainQuery);
        String graphSageStreamQuery = GdsCypher
            .call("newGraph")
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("modelName", graphSageModel)
            .yields();

        runQueryWithRowConsumer(graphSageStreamQuery, row -> {
            Collection<Double> embedding = (Collection<Double>) row.get("embedding");
            assertEquals(embedding.size(), embeddingDimension);

            double[] values = embedding.stream()
                .mapToDouble(Double::doubleValue)
                .toArray();
            assertNotEquals(0D, l2Norm(values));
        });
    }
}
