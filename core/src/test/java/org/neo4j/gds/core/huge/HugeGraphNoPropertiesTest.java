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
package org.neo4j.gds.core.huge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class HugeGraphNoPropertiesTest extends BaseTest {

    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node {name:'a'})" +
            ", (b:Node {name:'b'})" +
            ", (c:Node {name:'c'})" +
            ", (d:Node2 {name:'d'})" +
            ", (e:Node2 {name:'e'})" +
            ", (a)-[:TYPE {prop:1}]->(b)" +
            ", (e)-[:TYPE {prop:2}]->(d)" +
            ", (d)-[:TYPE {prop:3}]->(c)" +
            ", (a)-[:TYPE {prop:4}]->(c)" +
            ", (a)-[:TYPE {prop:5}]->(d)" +
            ", (a)-[:TYPE2 {prop:6}]->(d)" +
            ", (b)-[:TYPE2 {prop:7}]->(e)" +
            ", (a)-[:TYPE2 {prop:8}]->(e)";

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @ParameterizedTest
    @EnumSource(Orientation.class)
    void relationshipIteratorShouldReturnFallbackWeight(Orientation orientation) {
        Graph graph = loadGraph(db, orientation);

        double fallbackWeight = 42D;
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, fallbackWeight, (s, t, w) -> {
                assertEquals(fallbackWeight, w);
                return true;
            });
            return true;
        });
    }

    @ParameterizedTest
    @EnumSource(Orientation.class)
    void weightOfShouldReturnFallbackWeight(Orientation orientation) {
        Graph graph = loadGraph(db, orientation);

        double fallbackWeight = 42D;
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, (s, t) -> {
                assertEquals(fallbackWeight, graph.relationshipProperty(s, t, fallbackWeight));
                return true;
            });
            return true;
        });
    }

    private Graph loadGraph(final GraphDatabaseAPI db, Orientation orientation) {
        return new StoreLoaderBuilder()
            .databaseService(db)
            .globalOrientation(orientation)
            .build()
            .graph();
    }

}
