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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.GraphSageStreamProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageTrainProc;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateProc;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.pagerank.PageRankMutateProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateProc;
import org.neo4j.gds.wcc.WccMutateProc;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.math.L2Norm.l2Norm;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Neo4jModelCatalogExtension
class GraphMutateProcIntegrationTest extends BaseProcTest {

    private static final String TEST_GRAPH = "testGraph";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node { nodeId: 0.0 })" +
        ", (b:Node { nodeId: 1.0 })" +
        ", (c:Node { nodeId: 2.0 })" +
        ", (d:Node { nodeId: 3.0 })" +
        ", (e:Node { nodeId: 4.0 })" +
        ", (f:Node { nodeId: 5.0 })" +
        ", (g:Node { nodeId: 6.0 })" +
        ", (h:Node { nodeId: 7.0 })" +
        ", (i:Node { nodeId: 8.0 })" +
        ", (j:Node { nodeId: 9.0 })" +
        ", (k:Node { nodeId: 10.0 })" +
        ", (l:Node { nodeId: 11.0 })" +
        ", (a)-[:TYPE {p: 10}]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(g)" +
        ", (h)-[:TYPE]->(i)" +
        ", (i)-[:TYPE]->(k)" +
        ", (i)-[:TYPE]->(l)" +
        ", (j)-[:TYPE]->(k)";

    private static final Graph EXPECTED_GRAPH = fromGdl(
        "(a {nodeId: 0.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.150000, wcc: 0})" +
        "(b {nodeId: 1.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.277500, wcc: 0})" +
        "(c {nodeId: 2.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.385875, wcc: 0})" +
        "(d {nodeId: 3.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.477994, wcc: 0})" +
        "(e {nodeId: 4.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.556295, wcc: 0})" +
        "(f {nodeId: 5.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.622850, wcc: 0})" +
        "(g {nodeId: 6.0,  labelPropagation: 6,  louvain: 6,  pageRank: 0.679423, wcc: 0})" +
        "(h {nodeId: 7.0,  labelPropagation: 10, louvain: 10, pageRank: 0.150000, wcc: 7})" +
        "(i {nodeId: 8.0,  labelPropagation: 10, louvain: 10, pageRank: 0.277500, wcc: 7})" +
        "(j {nodeId: 9.0,  labelPropagation: 10, louvain: 10, pageRank: 0.150000, wcc: 7})" +
        "(k {nodeId: 10.0, labelPropagation: 10, louvain: 10, pageRank: 0.395438, wcc: 7})" +
        "(l {nodeId: 11.0, labelPropagation: 11, louvain: 10, pageRank: 0.267938, wcc: 7})" +
        "(a)-[:TYPE {w: 1.0}]->(b)" +
        "(b)-[:TYPE {w: 1.0}]->(c)" +
        "(c)-[:TYPE {w: 1.0}]->(d)" +
        "(d)-[:TYPE {w: 1.0}]->(e)" +
        "(e)-[:TYPE {w: 1.0}]->(f)" +
        "(f)-[:TYPE {w: 1.0}]->(g)" +
        "(h)-[:TYPE {w: 1.0}]->(i)" +
        "(i)-[:TYPE {w: 1.0}]->(k)" +
        "(i)-[:TYPE {w: 1.0}]->(l)" +
        "(j)-[:TYPE {w: 1.0}]->(k)" +
        // SIMILAR_TO
        "(i)-[:SIMILAR_TO {w: 0.5}]->(j)" +
        "(j)-[:SIMILAR_TO {w: 0.5}]->(i)"
    );

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(
            GraphProjectProc.class,
            GraphDropProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphWriteRelationshipProc.class,
            PageRankMutateProc.class,
            WccMutateProc.class,
            LabelPropagationMutateProc.class,
            LouvainMutateProc.class,
            NodeSimilarityMutateProc.class,
            GraphSageTrainProc.class,
            GraphSageStreamProc.class
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
        // run algorithms in mutate mode
        String pageRankQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("pageRank")
            .mutateMode()
            .addParameter("mutateProperty", "pageRank")
            .yields();
        String wccQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", "wcc")
            .yields();
        String labelPropagationQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("labelPropagation")
            .mutateMode()
            .addParameter("nodeWeightProperty", "pageRank")
            .addParameter("mutateProperty", "labelPropagation")
            .yields();
        String louvainQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("louvain")
            .mutateMode()
            .addParameter("mutateProperty", "louvain")
            .yields();
        String nodeSimilarityQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("nodeSimilarity")
            .mutateMode()
            .addParameter("mutateProperty", "similarity")
            .addParameter("mutateRelationshipType", "SIMILAR_TO")
            .yields();

        runQuery(pageRankQuery);
        runQuery(wccQuery);
        runQuery(labelPropagationQuery);
        runQuery(louvainQuery);
        runQuery(nodeSimilarityQuery);

        assertGraphEquals(EXPECTED_GRAPH, GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH).graphStore().getUnion());

        int embeddingDimension = 64;
        String graphSageModel = "graphSageModel";
        String graphSageTrainQuery = GdsCypher
            .call(TEST_GRAPH)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("pageRank", "louvain", "labelPropagation", "wcc"))
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("modelName", graphSageModel)
            .yields();

        runQuery(graphSageTrainQuery);

        String graphSageStreamQuery = GdsCypher
            .call(TEST_GRAPH)
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

        // write new properties and relationships to Neo
        String writeNodePropertiesQuery = formatWithLocale(
            "CALL gds.graph.nodeProperties.write(" +
            "   '%s', " +
            "   ['pageRank', 'louvain', 'labelPropagation', 'wcc']" +
            ")",
            TEST_GRAPH
        );
        runQueryWithRowConsumer(writeNodePropertiesQuery, row -> {
            assertEquals(row.getNumber("propertiesWritten").longValue(), 48L);
        });

        String writeRelationshipTypeQuery = formatWithLocale(
            "CALL gds.graph.relationship.write(" +
            "   '%s', " +
            "   'SIMILAR_TO', " +
            "   'similarity'" +
            ")",
            TEST_GRAPH
        );
        runQuery(writeRelationshipTypeQuery);

        // re-create named graph from written node and relationship properties
        runQuery(formatWithLocale("CALL gds.graph.drop('%s')", TEST_GRAPH));
        runQuery(GdsCypher.call(TEST_GRAPH)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("nodeId")
            .withNodeProperty("pageRank")
            .withNodeProperty("louvain")
            .withNodeProperty("wcc")
            .withNodeProperty("labelPropagation")
            .withRelationshipType("TYPE")
            .withRelationshipType("SIMILAR_TO")
            .withRelationshipProperty("similarity", DefaultValue.of(1.0))
            .yields()
        );

        assertGraphEquals(EXPECTED_GRAPH, GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH).graphStore().getUnion());
    }
}
