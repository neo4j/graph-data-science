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
package org.neo4j.graphalgo.core.write;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class RelationshipStreamExporterTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { id: 0 })" +
        ", (b { id: 1 })" +
        ", (c { id: 2 })" +
        ", (d { id: 3 })";

    private Graph graph;

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
        graph = new StoreLoaderBuilder().api(db).build().graphStore().getUnion();
    }

    long nodeId(String propertyKey, int propertyValue) {
        return (long) runQuery(formatWithLocale(
            "MATCH (n) WHERE n.%s = %d RETURN id(n) AS id",
            propertyKey,
            propertyValue
        ), result -> result.next().get("id"));
    }

    RelationshipStreamExporter.Relationship relationship(int sourceProperty, int targetProperty, Value... values) {
        return ImmutableRelationship.of(
            graph.toMappedNodeId(nodeId("id", sourceProperty)),
            graph.toMappedNodeId(nodeId("id", targetProperty)),
            values
        );
    }

    @Test
    void exportStream() {
        var exportRelationships = List.of(
            relationship(0, 1, Values.longValue(42L)),
            relationship(0, 2, Values.longValue(43L)),
            relationship(1, 0, Values.longValue(44L)),
            relationship(2, 2, Values.longValue(45L)),
            relationship(2, 3, Values.longValue(46L)),
            relationship(2, 3, Values.longValue(47L))
        );

        var exporter = RelationshipStreamExporter
            .builder(db, graph, exportRelationships.stream(), TerminationFlag.RUNNING_TRUE)
            .build();

        var relationshipType = "FOOBAR";
        var relationshipProperty = "x";
        var relationshipsWritten = exporter.write(relationshipType, relationshipProperty);

        assertEquals(exportRelationships.size(), relationshipsWritten);

        var exportedGraph = new StoreLoaderBuilder().api(db)
            .addRelationshipType(relationshipType)
            .addRelationshipProperty(relationshipProperty, relationshipProperty, DefaultValue.of(0), Aggregation.NONE)
            .build()
            .graphStore()
            .getUnion();

        assertGraphEquals(
            fromGdl("(a), (b), (c), (d), " +
                    "(a)-[:FOOBAR {x: 42}]->(b)" +
                    "(a)-[:FOOBAR {x: 43}]->(c)" +
                    "(b)-[:FOOBAR {x: 44}]->(a)" +
                    "(c)-[:FOOBAR {x: 45}]->(c)" +
                    "(c)-[:FOOBAR {x: 46}]->(d)" +
                    "(c)-[:FOOBAR {x: 47}]->(d)"),
            exportedGraph
        );
    }
}
