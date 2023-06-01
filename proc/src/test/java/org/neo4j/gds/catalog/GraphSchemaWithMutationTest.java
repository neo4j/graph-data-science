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
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateProc;
import org.neo4j.gds.wcc.WccMutateProc;

import java.util.Map;

import static java.util.Collections.singletonList;

class GraphSchemaWithMutationTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (:A {foo: 1})-[:REL {bar: 2}]->(:A)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphListProc.class,
            WccMutateProc.class,
            NodeSimilarityMutateProc.class
        );
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() { GraphStoreCatalog.removeAllLoadedGraphs();}

    @Test
    void listWithMutatedNodeProperty() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project($name, 'A', 'REL', {nodeProperties: 'foo', relationshipProperties: 'bar'})",
            Map.of("name", name)
        );
        runQuery(
            "CALL gds.wcc.mutate($name, {mutateProperty: 'baz'})",
            Map.of("name", name)
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            Map.of(
                "schema", Map.of(
                    "nodes", Map.of("A", Map.of(
                        "foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)",
                        "baz", "Integer (DefaultValue(-9223372036854775808), TRANSIENT)"
                    )),
                    "relationships", Map.of("REL", Map.of("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")),
                    "graphProperties",
                    Map.of()
                )
            )
        ));
    }

    @Test
    void listWithMutatedRelationshipProperty() {
        runQuery("CALL gds.graph.project('graph', 'A', 'REL', {nodeProperties: 'foo', relationshipProperties: 'bar'})");
        runQuery("CALL gds.nodeSimilarity.mutate('graph', {mutateProperty: 'faz', mutateRelationshipType: 'BOO'})");

        assertCypherResult("CALL gds.graph.list() YIELD schema",
            singletonList(
                Map.of(
                    "schema",
                    Map.of("nodes",
                        Map.of("A", Map.of("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                        "relationships",
                        Map.of("BOO", Map.of("faz", "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)"),
                            "REL", Map.of("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")),
                        "graphProperties",
                        Map.of()
                    )
                )
            )
        );
    }

}
