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
package org.neo4j.gds.cliquecounting;

import org.assertj.core.api.AssertionsForClassTypes;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CliqueCountingMutateProcTest extends BaseProcTest {

    //todo

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String CLIQUE_COUNTING_GRAPH = "myGraph";
    private static final String MUTATE_PROPERTY = "perNodeCount";

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(b)" +
        ",(c)" +
        ",(d)" +
        ",(e)" +
        ",(a)-[:REL]->(b)" +
        ",(a)-[:REL]->(c)" +
        ",(a)-[:REL]->(d)" +
        ",(b)-[:REL]->(c)" +
        ",(b)-[:REL]->(d)" +
        ",(c)-[:REL]->(d)" +
        ",(a)-[:REL]->(e)" +
        ",(b)-[:REL]->(e)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            CliqueCountingMutateProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );
        runQuery(
            GdsCypher.call(CLIQUE_COUNTING_GRAPH)
                .graphProject()
                .loadEverything(Orientation.UNDIRECTED)
                .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private String expectedMutatedGraph() {
        return
            "  (a { perNodeCount: [4L,1L] }) " +
            ", (b { perNodeCount: [4L,1L] }) " +
            ", (c { perNodeCount: [3L,1L] }) " +
            ", (d { perNodeCount: [3L,1L] }) " +
            ", (e { perNodeCount: [1L]    }) " +
            ", (a)-->(b) " +
            ", (a)-->(c) " +
            ", (a)-->(d) " +
            ", (b)-->(c) " +
            ", (b)-->(d) " +
            ", (c)-->(d) " +
            ", (a)-->(e) " +
            ", (b)-->(e) ";
    }

    @Test
    void testMutate() {
        @Language("Cypher")
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds.cliqueCounting")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        var rowCount=runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("nodePropertiesWritten").longValue()).isEqualTo(5);
            assertThat((List<Long>) row.get("globalCount")).containsExactly(5L,1L);
        });

        assertThat(rowCount).isEqualTo(1L);

        var graphStore = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db.databaseName()), CLIQUE_COUNTING_GRAPH).graphStore();

        var mutatedGraph=graphStore.getUnion();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph(), Orientation.UNDIRECTED), mutatedGraph);

        var containsMutateProperty =  graphStore.schema().nodeSchema()
            .entries()
            .stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .anyMatch(
                props -> props.getKey().equals(MUTATE_PROPERTY) &&
                    props.getValue().valueType() == ValueType.LONG_ARRAY
            );
        assertThat(containsMutateProperty).isTrue();
    }

    @Disabled
    void testMutateEstimate() {
        @Language("Cypher")
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds.cliqueCounting")
            .mutateEstimation()
            .addParameter("mutateProperty", "globalCount")
            .yields("nodePropertiesWritten", "bytesMin", "bytesMax", "requiredMemory");

        assertCypherResult(query, List.of(Map.of(  //todo
            "nodePropertiesWritten", 4L,
            "bytesMin", 544L,
            "bytesMax", 544L,
            "requiredMemory", "544 Bytes"
        )));
    }

    @Test
    void testWriteBackGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (:B), (a1)-[:REL1]->(a2), (a2)-[:REL2]->(b)");
        String  projectQuery = GdsCypher.call(CLIQUE_COUNTING_GRAPH)
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withRelationshipType("REL1",Orientation.UNDIRECTED)
            .withRelationshipType("REL2",Orientation.UNDIRECTED)
            .yields();
        runQuery(projectQuery);

        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds", "cliqueCounting")
            .mutateMode()
            .addParameter("nodeLabels", Collections.singletonList("B"))
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(query);

        String graphWriteQuery =
            "CALL gds.graph.nodeProperties.write(" +
            "   $graph, " +
            "   [$property]" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten";

        runQuery(graphWriteQuery, Map.of("graph", CLIQUE_COUNTING_GRAPH, "property", MUTATE_PROPERTY));

        String checkNeo4jGraphNegativeQuery = formatWithLocale("MATCH (n:A) RETURN n.%s AS property", MUTATE_PROPERTY);

        var rowCountA=runQueryWithRowConsumer(
            checkNeo4jGraphNegativeQuery,
            (resultRow) -> assertNull(resultRow.get("property"))
        );

        assertThat(rowCountA).isEqualTo(2L);

        String checkNeo4jGraphPositiveQuery = formatWithLocale("MATCH (n:B) RETURN n.%s AS property", MUTATE_PROPERTY);

        var rowCountB= runQueryWithRowConsumer(
            checkNeo4jGraphPositiveQuery,
            (resultRow) -> assertNotNull(resultRow.get("property"))
        );

        assertThat(rowCountB).isEqualTo(2L);

    }



    @Test
    void testGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (a1)-[:REL]->(a2)");
        String  projectQuery = GdsCypher.call(CLIQUE_COUNTING_GRAPH)
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withRelationshipType("REL",Orientation.UNDIRECTED)
            .yields();
        runQuery(projectQuery);


        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds", "cliqueCounting")
            .mutateMode()
            .addParameter("nodeLabels", Collections.singletonList("A"))
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(query);

        var mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db.databaseName()), CLIQUE_COUNTING_GRAPH).graphStore();

        var expectedProperties = Set.of(MUTATE_PROPERTY);
        assertEquals(expectedProperties, mutatedGraph.nodePropertyKeys(NodeLabel.of("A")));
        assertEquals(Set.of(), mutatedGraph.nodePropertyKeys(NodeLabel.of("B")));
    }

    @Test
    void testMutateFailsOnExistingToken() {
        String query = GdsCypher.call(CLIQUE_COUNTING_GRAPH).algo("gds", "cliqueCounting")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();
        runQuery(query);

        assertThatException().isThrownBy( () -> runQuery(query)).withMessageContaining(formatWithLocale(
        "Node property `%s` already exists in the in-memory graph.",
        MUTATE_PROPERTY
        ));
    }

    @Test
    void testRunOnEmptyGraph() {
        // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later

        runQuery("CALL db.createLabel('X')");
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        String  projectQuery = GdsCypher.call("foo")
            .graphProject().loadEverything(Orientation.UNDIRECTED).yields();
        runQuery(projectQuery);

        String query = GdsCypher.call("foo")
            .algo("gds", "cliqueCounting")
            .mutateMode()
            .addParameter("mutateProperty","foo2")
            .yields();

        var rowCount=runQueryWithRowConsumer(query, row -> {
            AssertionsForClassTypes.assertThat(row.getNumber("preProcessingMillis").longValue()).isNotEqualTo(-1);
            AssertionsForClassTypes.assertThat(row.getNumber("computeMillis").longValue()).isEqualTo(-1);
            AssertionsForClassTypes.assertThat(row.getNumber("nodePropertiesWritten").longValue()).isEqualTo(0);
            AssertionsForClassTypes.assertThat(((List<Long>) row.get("globalCount")).size()).isEqualTo(0);
        });

        AssertionsForClassTypes.assertThat(rowCount).isEqualTo(1L);
    }



}
