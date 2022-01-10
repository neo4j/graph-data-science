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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.GraphFactoryTestSupport.FactoryType.NATIVE;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface MutatePropertyProcTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends MutateConfig & AlgoBaseConfig, RESULT>
    extends MutateProcTest<ALGORITHM, CONFIG, RESULT> {

    String mutateProperty();

    ValueType mutatePropertyType();

    @Override
    default CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", mutateProperty());
        }
        return mapWrapper;
    }

    @Override
    @Test
    default void testGraphMutation() {
        GraphStore graphStore = runMutation();

        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());
        GraphSchema schema = graphStore.schema();
        if (mutateProperty() != null) {
            boolean nodesContainMutateProperty = containsMutateProperty(schema.nodeSchema().properties());
            boolean relationshipsContainMutateProperty = containsMutateProperty(schema
                .relationshipSchema()
                .properties());
            assertTrue(nodesContainMutateProperty || relationshipsContainMutateProperty);
        }
    }

    default <PS extends PropertySchema> boolean containsMutateProperty(Map<?, Map<String, PS>> entitySchema) {
        return entitySchema
            .values()
            .stream()
            .anyMatch(props -> props.containsKey(mutateProperty()) && props.get(mutateProperty()).valueType() == mutatePropertyType());
    }

    @Test
    default void testGraphMutationOnFilteredGraph() {
        runQuery(graphDb(), "MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery(graphDb(), "CREATE (a1: A), (a2: A), (b: B), (a1)-[:REL]->(a2)");
        GraphStore graphStore = TestGraphLoaderFactory.graphLoader(graphDb(), NATIVE)
            .withLabels("A", "B")
            .withRelationshipTypes("REL")
            .graphStore();

        String graphName = "myGraph";
        var graphProjectConfig = withNameAndRelationshipProjections("", graphName, relationshipProjections());
        GraphStoreCatalog.set(graphProjectConfig, graphStore);

        CypherMapWrapper filterConfig = CypherMapWrapper.empty().withEntry(
            "nodeLabels",
            Collections.singletonList("A")
        );

        runMutation(graphName, filterConfig);

        GraphStore mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore();
        Assertions.assertEquals(
            Collections.singleton(mutateProperty()),
            mutatedGraph.nodePropertyKeys(NodeLabel.of("A"))
        );

        assertEquals(
            Collections.emptySet(),
            mutatedGraph.nodePropertyKeys(NodeLabel.of("B"))
        );
    }

    @Override
    @Test
    default void testMutateFailsOnExistingToken() {
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

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore().getUnion();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Override
    default String failOnExistingTokenMessage() {
        return formatWithLocale(
            "Node property `%s` already exists in the in-memory graph.",
            mutateProperty()
        );
    }
}
