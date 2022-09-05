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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.warnings.GlobalUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryExtension;
import org.neo4j.gds.degree.DegreeCentralityMutateProc;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class GraphDropNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
        ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
        ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
        ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
        ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
        ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    GlobalUserLogStore userLogStore;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        this.userLogStore = new GlobalUserLogStore();
        builder.removeExtensions(extension -> extension instanceof UserLogRegistryExtension);
        builder.addExtension(new UserLogRegistryExtension(() -> userLogStore));
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphDropNodePropertiesProc.class,
            FastRPMutateProc.class,
            DegreeCentralityMutateProc.class
        );
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call(TEST_GRAPH_SAME_PROPERTIES)
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeProperty("nodeProp1")
            .withNodeProperty("nodeProp2")
            .withAnyRelationshipType()
            .yields()
        );

        runQuery(GdsCypher.call(TEST_GRAPH_DIFFERENT_PROPERTIES)
            .graphProject()
            .withNodeLabel("A", NodeProjection.of(
                "A",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("nodeProp1", 1337),
                    PropertyMapping.of("nodeProp2", 1337)
                )
            ))
            .withNodeLabel("B", NodeProjection.of(
                "B",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("nodeProp1", 1337)
                )
            ))
            .withAnyRelationshipType()
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void removeNodeProperties() {
        assertCypherResult(
            "CALL gds.graph.nodeProperties.drop($graphName, ['nodeProp1', 'nodeProp2'])",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES),
            List.of(Map.of(
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", List.of("nodeProp1", "nodeProp2"),
                "propertiesRemoved", 12L
            ))
        );
    }

    @Test
    void removeNodePropertyShouldComplainWithInvalidInput() {
        assertError(
            "CALL gds.graph.nodeProperties.drop($graphName, [['nodeProp1']])",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES),
            "Type mismatch"
        );
    }

    @Test
    void removeNodePropertyAsString() {
        assertCypherResult(
            "CALL gds.graph.nodeProperties.drop($graphName, 'nodeProp1')",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES),
            List.of(Map.of(
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", List.of("nodeProp1"),
                "propertiesRemoved", 6L
            ))
        );
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        assertError(
            "CALL gds.graph.nodeProperties.drop($graphName, ['nodeProp1', 'nodeProp2', 'nodeProp3'])",
            Map.of("graphName", TEST_GRAPH_DIFFERENT_PROPERTIES),
            "Could not find property key(s) ['nodeProp3']. Defined keys: ['nodeProp1', 'nodeProp2']"
        );
    }

    @Test
    void shouldReportRemovalOfFastRPProperties() {
        var fastRPCall = GdsCypher
            .call(TEST_GRAPH_SAME_PROPERTIES)
            .algo("fastRP")
            .mutateMode()
            .addParameter("mutateProperty", "fastrp")
            .addParameter("embeddingDimension", 1)
            .yields();

        runQuery(fastRPCall);

        List<String> propertiesToRemove = List.of("fastrp", "nodeProp1", "nodeProp2");

        assertCypherResult(
            "CALL gds.graph.nodeProperties.drop( $graphName, $nodeProperties)",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES, "nodeProperties", propertiesToRemove),
            List.of(Map.of(
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", propertiesToRemove,
                "propertiesRemoved", 18L
            ))
        );

        var graphStore = GraphStoreCatalog
            .get(CatalogRequest.of(getUsername(), db.databaseName()), TEST_GRAPH_SAME_PROPERTIES)
            .graphStore();

        assertThat(propertiesToRemove).allMatch(property -> graphStore.nodeLabels().stream().noneMatch(label -> graphStore.hasNodeProperty(label, property)));
    }

    @Test
    void shouldLogDeprecationWarning() {
        runQuery("CALL gds.graph.removeNodeProperties($graph, ['nodeProp1'])", Map.of("graph", TEST_GRAPH_SAME_PROPERTIES));
        var userLogEntries = userLogStore.query(getUsername()).collect(Collectors.toList());
        Assertions.assertThat(userLogEntries.size()).isEqualTo(1);
        Assertions.assertThat(userLogEntries.get(0).getMessage())
            .contains("deprecated")
            .contains("gds.graph.nodeProperties.drop");
    }
}
