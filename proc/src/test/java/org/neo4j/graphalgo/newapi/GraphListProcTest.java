/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.newapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.GraphLoadProc;
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.neo4j.helpers.collection.MapUtil.map;

class GraphListProcTest extends ProcTestBase {

    private static final String DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCatalogProcs.class,
            GraphLoadProc.class
        );
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void listASingleGraph() {
        String name = "name";
        db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL algo.beta.graph.list()", singletonList(
            map(
                "graphName", name,
                "nodeProjection", map(
                    "A", map(
                        "label", "A",
                        "properties", emptyMap()
                    )
                ),
                "relationshipProjection", map(
                    "REL", map(
                        "type", "REL",
                        "projection", "NATURAL",
                        "aggregation", "DEFAULT",
                        "properties", emptyMap()
                    )
                ),
                "nodes", 2L,
                "relationships", 2L,
                "histogram", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Test
    void histogramComputationIsOptOut() {
        String name = "name";
        db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL algo.beta.graph.list() YIELD graphName, nodeProjection, relationshipProjection, nodes, relationships", singletonList(
            map(
                "graphName", name,
                "nodeProjection", map(
                    "A", map(
                        "label", "A",
                        "properties", emptyMap()
                    )
                ),
                "relationshipProjection", map(
                    "REL", map(
                        "type", "REL",
                        "projection", "NATURAL",
                        "aggregation", "DEFAULT",
                        "properties", emptyMap()
                    )),
                "nodes", 2L,
                "relationships", 2L
            )
        ));
    }

    @Test
    void calculateHistogramForUndirectedNodesWhenAskedTo() {
        String name = "name";
        db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL algo.beta.graph.list() YIELD histogram", singletonList(
            map(
                "histogram", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Disabled("Disabled until we support REL> syntax for type filter")
    @Test
    void calculateHistogramForOutgoingRelationshipsWhenAskedTo() {
        String name = "name";
        db.execute("CALL algo.beta.graph.create($name, 'A', 'REL>')", map("name", name));

        assertCypherResult("CALL algo.beta.graph.list() YIELD histogram", singletonList(
            map(
                "histogram", map(
                    "min", 1,
                    "mean", 1,
                    "max", 1,
                    "p50", 1,
                    "p75", 1,
                    "p90", 1,
                    "p95", 1,
                    "p99", 1,
                    "p999", 1
                )
            )
        ));
    }

    @Disabled("Disabled until we support REL> syntax for type filter")
    @Test
    void calculateHistogramForIncomingRelationshipsWhenAskedTo() {
        String name = "name";
        db.execute("CALL algo.beta.graph.create($name, 'A', '<REL')", map("name", name));

        assertCypherResult("CALL algo.beta.graph.list() YIELD histogram", singletonList(
            map(
                "histogram", map(
                    "min", 1,
                    "mean", 1,
                    "max", 1,
                    "p50", 1,
                    "p75", 1,
                    "p90", 1,
                    "p95", 1,
                    "p99", 1,
                    "p999", 1
                )
            )
        ));
    }

    @ParameterizedTest(name = "name argument: {0}")
    @ValueSource(strings = {"", "''"})
    void listAllGraphsWhenCalledWithoutArgumentOrAnEmptyArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", name));
        }

        List<String> actualNames = db
            .execute("CALL algo.beta.graph.list(" + argument + ")")
            .<String>columnAs("graphName")
            .stream()
            .collect(toList());

        assertThat(actualNames, containsInAnyOrder(names));
    }

    @Test
    void failForNullNameArgument() {
        assertError("CALL algo.beta.graph.list(null)", "No value specified for the mandatory configuration parameter `graphName`");
    }

    @Test
    void filterOnExactMatchUsingTheFirstArgument() {
        String[] names = {"b", "bb", "ab", "ba", "B", "ʙ"};
        for (String name : names) {
            db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", name));
        }

        String name = names[0];
        List<String> actualNames = db
            .execute("CALL algo.beta.graph.list($name)", map("name", name))
            .<String>columnAs("graphName")
            .stream()
            .collect(toList());

        assertThat(actualNames.size(), is(1));
        assertThat(actualNames, contains(name));
    }

    @Test
    void returnEmptyStreamWhenNoGraphsAreLoaded() {
        long numberOfRows = db
            .execute("CALL algo.beta.graph.list()")
            .stream()
            .count();

        assertThat(numberOfRows, is(0L));
    }

    @ParameterizedTest(name = "name argument: ''{0}''")
    @ValueSource(strings = {"foobar", " "})
    void returnEmptyStreamWhenNoGraphMatchesTheFilterArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            db.execute("CALL algo.beta.graph.create($name, 'A', 'REL')", map("name", name));
        }

        long numberOfRows = db
            .execute("CALL algo.beta.graph.list($argument)", map("argument", argument))
            .stream()
            .count();

        assertThat(numberOfRows, is(0L));
    }
}
