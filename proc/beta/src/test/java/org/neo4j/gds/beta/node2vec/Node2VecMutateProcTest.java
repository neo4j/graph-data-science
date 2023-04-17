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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class Node2VecMutateProcTest extends BaseProcTest
    implements
        AlgoBaseProcTest<Node2Vec, Node2VecMutateConfig, Node2VecModel.Result>,
        MutateNodePropertyTest<Node2Vec, Node2VecMutateConfig, Node2VecModel.Result> {

    @Neo4jGraph
    public static final String DB_CYPHER =
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
    void loadProcedures() throws Exception {
        registerProcedures(
            Node2VecMutateProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );
    }

    @Override
    public Class<Node2VecMutateProc> getProcedureClazz() {
        return Node2VecMutateProc.class;
    }

    @Override
    public Node2VecMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return Node2VecMutateConfig.of(mapWrapper);
    }

    @Override
    public String mutateProperty() {
        return "node2vecEmbedding";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.FLOAT_ARRAY;
    }

    @Override
    @Test
    public void testGraphMutation() {
        assertMutatedGraph(runMutation());
    }

    @Override
    @Test
    public void testMutateFailsOnExistingToken() {
        String graphName = ensureGraphExists();

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    Map<String, Object> config = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    try {
                        // write first time
                        mutateMethod.invoke(procedure, graphName, config);
                        // write second time using same `writeProperty`
                        assertThatThrownBy(() -> mutateMethod.invoke(procedure, graphName, config))
                            .hasRootCauseInstanceOf(IllegalArgumentException.class)
                            .hasRootCauseMessage(failOnExistingTokenMessage());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        GraphStore mutatedGraphStore = GraphStoreCatalog.get(AlgoBaseProcTest.TEST_USERNAME, databaseId(), graphName).graphStore();
        assertMutatedGraph(mutatedGraphStore);
    }

    @Test
    void returnLossPerIteration() {
        loadGraph(DEFAULT_GRAPH_NAME);
        int iterations = 5;
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.node2vec")
            .mutateMode()
            .addParameter("embeddingDimension", 42)
            .addParameter("mutateProperty", "testProp")
            .addParameter("iterations", iterations)
            .yields("lossPerIteration");

        assertCypherResult(query, List.of(Map.of("lossPerIteration", Matchers.hasSize(iterations))));
    }

    private void assertMutatedGraph(GraphStore mutatedGraphStore) {
        var mutatedProperties = mutatedGraphStore.nodeProperty(mutateProperty()).values();
        mutatedGraphStore.nodes().forEachNode(nodeId -> {
            var embedding = mutatedProperties.floatArrayValue(nodeId);
            assertThat(embedding)
                .hasSize(128)
                .satisfies(array -> {
                    var allZeros = true;
                    for (float v : array) {
                        allZeros &= v == 0.0f;
                    }
                    assertThat(allZeros)
                        .withFailMessage("Embedding %s should not be all zeroes", Arrays.toString(array))
                        .isFalse();
                });

            return true;
        });

        GraphSchema schema = mutatedGraphStore.schema();
        if (mutateProperty() != null) {
            boolean nodesContainMutateProperty = containsMutateProperty(schema.nodeSchema());
            assertThat(nodesContainMutateProperty)
                .withFailMessage(
                    "The node schema %s should contain a property called `%s` of type `%s`",
                    mutatedGraphStore.schema().nodeSchema(),
                    mutateProperty(),
                    mutatePropertyType()
                )
                .isTrue();
        }
    }

    @Override
    public String expectedMutatedGraph() {
        throw new UnsupportedOperationException("Node2Vec is a random based algorithm and cannot support this method");
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(Node2VecModel.Result result1, Node2VecModel.Result result2) {
        // TODO: This just tests that the dimensions are the same for node 0, it's not a very good equality test
        assertEquals(result1.embeddings().get(0).data().length, result2.embeddings().get(0).data().length);
    }

    @Test
    @Disabled("Mutate on empty graph has not been covered in AlgoBaseProcTest 🙈")
    public void testRunOnEmptyGraph() {
    }
}
