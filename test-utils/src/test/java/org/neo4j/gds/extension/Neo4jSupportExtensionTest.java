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
package org.neo4j.gds.extension;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.QueryRunner;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class Neo4jSupportExtensionTest extends BaseTest {

    // The full identifier is used here to show that this
    // import is explicitly from Neo4j as opposed to our
    // own Inject interface, in order to compare both results.
    @org.neo4j.test.extension.Inject
    GraphDatabaseService neoInjectedDb;

    @Neo4jGraph(offsetIds = true)
    static final String DB_CYPHER = "CREATE" +
                                            "  (a { id: 0 })" +
                                            ", (b { id: 1 })";

    @Test
    void shouldLoadGraphAndIdFunctionThroughExtension() {
        assertNotNull(db);
        assertEquals(db, neoInjectedDb);
        long idA = idFunction.of("a");
        long idB = idFunction.of("b");
        assertEquals(nodeIdByProperty(db, 0), idA);
        assertEquals(nodeIdByProperty(db, 1), idB);
    }

    @Test
    void shouldInjectNodeFunction() {
        var nodeA = nodeFunction.of("a");
        var nodeB = nodeFunction.of("b");

        QueryRunner.runQueryWithRowConsumer(
            db,
            "MATCH (n) WHERE n.id = 0 RETURN n",
            Map.of(),
            (tx, row) -> assertEquals(nodeA, row.getNode("n"))
        );
        QueryRunner.runQueryWithRowConsumer(
            db,
            "MATCH (n) WHERE n.id = 1 RETURN n",
            Map.of(),
            (tx, row) -> assertEquals(nodeB, row.getNode("n"))
        );
    }

    @Test
    void shouldOffsetIds() {
        QueryRunner.runQueryWithRowConsumer(
            db,
            "MATCH (n) RETURN id(n) AS id",
            Map.of(),
            (tx, row) -> assertThat(row.getNumber("id").longValue()).isGreaterThanOrEqualTo(2)
        );
    }

    static long nodeIdByProperty(GraphDatabaseService db, long propertyValue) {
        var nodeId = new MutableLong(0L);
        QueryRunner.runQueryWithRowConsumer(
            db,
            formatWithLocale("MATCH (n) WHERE n.id = %d RETURN id(n) AS id", propertyValue),
            resultRow -> nodeId.setValue(resultRow.getNumber("id"))
        );
        return nodeId.longValue();
    }
}
