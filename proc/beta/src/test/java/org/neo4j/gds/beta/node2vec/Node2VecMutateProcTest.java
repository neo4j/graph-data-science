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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.ml.core.tensor.FloatVector;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.MutateNodePropertyTest;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

class Node2VecMutateProcTest
    extends Node2VecProcTest<Node2VecMutateConfig>
    implements MutateNodePropertyTest<Node2Vec, Node2VecMutateConfig, HugeObjectArray<FloatVector>> {

    @BeforeEach
    void loadProcedures() throws Exception {
        registerProcedures(
            GraphWriteNodePropertiesProc.class
        );
    }

    @Override
    public Class<? extends AlgoBaseProc<Node2Vec, HugeObjectArray<FloatVector>, Node2VecMutateConfig>> getProcedureClazz() {
        return Node2VecMutateProc.class;
    }

    @Override
    public Node2VecMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return Node2VecMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
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

        GraphStore mutatedGraphStore = GraphStoreCatalog.get(AlgoBaseProcTest.TEST_USERNAME, namedDatabaseId(), graphName).graphStore();
        assertMutatedGraph(mutatedGraphStore);
    }

    private void assertMutatedGraph(GraphStore mutatedGraphStore) {
        var mutatedProperties = mutatedGraphStore.nodePropertyValues(mutateProperty());
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
            boolean nodesContainMutateProperty = containsMutateProperty(schema.nodeSchema().properties());
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
}
