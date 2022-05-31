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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Fail.fail;

class FilteredKnnStreamProcTest extends FilteredKnnProcTest<FilteredKnnStreamConfig> {
    @Override
    public Class<? extends AlgoBaseProc<FilteredKnn, FilteredKnnResult, FilteredKnnStreamConfig, ?>> getProcedureClazz() {
        return FilteredKnnStreamProc.class;
    }

    @Override
    public FilteredKnnStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return FilteredKnnStreamConfig.of(mapWrapper);
    }

    @Test
    void shouldStreamResults() {
        String query = "CALL gds.alpha.knn.filtered.stream($graph, {nodeProperties: ['knn'], topK: 1, randomSeed: 19, concurrency: 1})" +
                       " YIELD node1, node2, similarity" +
                       " RETURN node1, node2, similarity" +
                       " ORDER BY node1";

        assertCypherResult(query, Map.of("graph", GRAPH_NAME), List.of(
            Map.of("node1", 0L, "node2", 1L, "similarity", 0.5),
            Map.of("node1", 1L, "node2", 0L, "similarity", 0.5),
            Map.of("node1", 2L, "node2", 1L, "similarity", 0.25)
        ));
    }

    @Test
    void shouldStreamWithFilteredNodes() {
        String nodeCreateQuery =
            "CREATE " +
            "  (alice:Person {age: 24})" +
            " ,(carol:Person {age: 24})" +
            " ,(eve:Person {age: 67})" +
            " ,(dave:Foo {age: 48})" +
            " ,(bob:Foo {age: 48})";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeProperty("age")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.knn.filtered")
            .streamMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age"))
            .yields("node1", "node2", "similarity");
        assertCypherResult(algoQuery, List.of(
            Map.of("node1", 6L, "node2", 7L, "similarity", 1.0),
            Map.of("node1", 7L, "node2", 6L, "similarity", 1.0)
        ));
    }

    @Test
    void shouldEmploySourceNodeFilter() {
        String nodeCreateQuery =
            "CREATE " +
            "  (alice:Person {age: 24})" +
            " ,(carol:Person {age: 24})" +
            " ,(eve:Person {age: 67})" +
            " ,(dave:Foo {age: 48})" +
            " ,(bob:Foo {age: 48})";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeProperty("age")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.knn.filtered")
            .streamMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age"))
            .addParameter("sourceNodeFilter", List.of(6L))
            .yields("node1", "node2", "similarity");

        assertCypherResult(algoQuery, List.of(
            Map.of("node1", 6L, "node2", 7L, "similarity", 1.0)
        ));
    }

    @Test
    void shouldEmployTargetNodeFilter() {
        String nodeCreateQuery =
            "CREATE " +
            "  (alice:Person {age: 24})" +
            " ,(carol:Person {age: 24})" +
            " ,(eve:Person {age: 67})" +
            " ,(dave:Foo {age: 48})" +
            " ,(bob:Foo {age: 48})";

        runQuery(nodeCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeProperty("age")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.alpha.knn.filtered")
            .streamMode()
            .addParameter("nodeLabels", List.of("Foo"))
            .addParameter("nodeProperties", List.of("age"))
            .addParameter("targetNodeFilter", List.of(7L))
            .yields("node1", "node2", "similarity");
        assertCypherResult(algoQuery, List.of(
            Map.of("node1", 6L, "node2", 7L, "similarity", 1.0)
        ));
    }

    @Disabled
    @Test
    void shouldSeed() {
        fail("Hopeless to try and test here, see FilteredKnnFactoryTest");
    }
}
