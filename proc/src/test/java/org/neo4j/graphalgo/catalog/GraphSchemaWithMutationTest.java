/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.nodesim.NodeSimilarityMutateProc;
import org.neo4j.graphalgo.wcc.WccMutateProc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.compat.MapUtil.map;

public class GraphSchemaWithMutationTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (:A {foo: 1})-[:REL {bar: 2}]->(:A)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCreateProc.class,
            GraphListProc.class,
            WccMutateProc.class,
            NodeSimilarityMutateProc.class
        );
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }
    @Test
    void listWithMutatedNodeProperty() {
        String name = "name";
        runQuery(
            "CALL gds.graph.create($name, 'A', 'REL', {nodeProperties: 'foo', relationshipProperties: 'bar'})",
            map("name", name)
        );
        runQuery(
            "CALL gds.wcc.mutate($name, {mutateProperty: 'baz'})",
            map("name", name)
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            map(
                "schema", map("nodeLabels", singletonList("A"),
                    "nodePropertyKeys", Arrays.asList("foo", "baz"),
                    "relationshipTypes", singletonList("REL"),
                    "relationshipPropertiesMap", map("REL", singletonList("bar"))
                )
            )
        ));
    }

    @Test
    void listWithMutatedRelationshipProperty() {
        String name = "name";
        runQuery(
            "CALL gds.graph.create($name, 'A', 'REL', {nodeProperties: 'foo', relationshipProperties: 'bar'})",
            map("name", name)
        );
        runQuery(
            "CALL gds.nodeSimilarity.mutate($name, {mutateProperty: 'faz', mutateRelationshipType: 'BOO'})",
            map("name", name)
        );

        runQueryWithRowConsumer("CALL gds.graph.list() YIELD schema RETURN schema.nodeLabels AS nodeLabels, " +
                                "schema.nodePropertyKeys AS nodePropertyKeys, " +
                                "schema.relationshipTypes AS relationshipTypes, " +
                                "schema.relationshipPropertiesMap AS relationshipPropertiesMap",
            resultRow -> {
                assertEquals(resultRow.get("nodeLabels"), singletonList("A"));
                assertEquals(resultRow.get("nodePropertyKeys"), singletonList("foo"));
                Set<String> relationshipTypes = new HashSet<>((List<String>) resultRow.get("relationshipTypes"));
                assertEquals(new HashSet<>(Arrays.asList("BOO", "REL")), relationshipTypes);
                assertEquals(resultRow.get("relationshipPropertiesMap"), map("REL", singletonList("bar"), "BOO", singletonList("faz")));
            }
        );
    }

}
