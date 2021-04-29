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
package org.neo4j.gds.paths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphdb.RelationshipType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.paths.PathFactory.DEFAULT_RELATIONSHIP_OFFSET;

class PathFactoryTest extends BaseProcTest {

    @Nested
    class SingleNode {
        static final String DB_CYPHER = "CREATE ()";

        @BeforeEach
        void setup() {
            runQuery(DB_CYPHER);
        }

        @Test
        void emptyPath() {
            var nodeIds = new long[]{0L};
            var costs = new double[]{0.0D};

            GraphDatabaseApiProxy.runInTransaction(db, tx -> {
                var path = PathFactory.create(
                    tx,
                    DEFAULT_RELATIONSHIP_OFFSET,
                    nodeIds,
                    costs,
                    RelationshipType.withName("REL"),
                    "prop"
                );
                assertEquals(0, path.length());
            });
        }
    }

    @Nested
    class MultipleNodes {
        static final String DB_CYPHER =
            "CREATE" +
            "  (a)" +
            ", (b)" +
            ", (c)" +
            ", (a)-[:R]->(b)" +
            ", (b)-[:R]->(c)";

        @BeforeEach
        void setup() {
            runQuery(DB_CYPHER);
        }

        @Test
        void pathWithCosts() {
            var nodeIds = new long[]{0L, 1L, 2L};
            var costs = new double[]{0.0D, 1.0D, 4.0D};

            GraphDatabaseApiProxy.runInTransaction(db, tx -> {
                var path = PathFactory.create(
                    tx,
                    DEFAULT_RELATIONSHIP_OFFSET,
                    nodeIds,
                    costs,
                    RelationshipType.withName("REL"),
                    "prop"
                );

                assertEquals(2, path.length());

                path.relationships().forEach(relationship -> {
                    var actualCost = (double) relationship.getProperty("prop");
                    var expectedCost = costs[(int) relationship.getEndNodeId()] - costs[(int) relationship.getStartNodeId()];

                    assertEquals(expectedCost, actualCost, 1E-4);
                });
            });
        }
    }
}
