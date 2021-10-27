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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.GraphDbApi;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.NativeFactory;

import java.util.Optional;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

public class NodeFilteredGraphTest {

    private GraphDbApi db;

    private static final String DB_CYPHER =
        " CREATE" +
        " (a:Person)," +
        " (b:Ignore:Person)," +
        " (c:Ignore:Person)," +
        " (d:Person)," +
        " (e:Ignore)," +
        " (a)-[:REL]->(b)," +
        " (a)-[:REL]->(e)";

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, DB_CYPHER);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }


    @Test
    void filterDegree() {
        GraphStore graphStore = new StoreLoaderBuilder()
            .api(db)
            .nodeLabels(asList("Person", "Ignore"))
            .addRelationshipType("REL")
            .build()
            .graphStore(NativeFactory.class);

        Graph graph = graphStore.getGraph(
            asList(NodeLabel.of("Person")),
            asList(RelationshipType.of("REL")),
            Optional.empty(),
            4
        );

        long nodeIdOfA = graph.toMappedNodeId(0);
        assertEquals(1L, graph.degree(nodeIdOfA));
    }

}
