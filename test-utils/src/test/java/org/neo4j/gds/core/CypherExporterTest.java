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
package org.neo4j.gds.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class CypherExporterTest extends BaseTest {

    private static final String newLine = System.lineSeparator();

    @BeforeEach
    void setup() {
        String createGraph =
                "CREATE (nA:Label1 { foo: 'bar' }) " +
                "CREATE (nB:Label1 { property: 42.1337 }) " +
                "CREATE (nC:Label2) " +
                "CREATE (nD) " +
                "CREATE " +
                "  (nA)-[:TYPE1 {bar: 'baz'}]->(nB), " +
                "  (nA)-[:TYPE1 {property:1337.42}]->(nC), " +
                "  (nB)-[:TYPE2]->(nC), " +
                "  (nC)-[:TYPE]->(nD)";

        runQuery(createGraph);
    }

    @Test
    void testDumpByGraphDbApi() {
        StringWriter output = new StringWriter();
        CypherExporter.export(new PrintWriter(output), db);

        //language=Cypher
        String expected =
                "CREATE (n0:Label1 {foo:bar})" + newLine +
                "CREATE (n1:Label1 {property:42.1337})" + newLine +
                "CREATE (n2:Label2)" + newLine +
                "CREATE (n3)" + newLine +
                "CREATE" + newLine +
                "  (n0)-[:TYPE1 {property:1337.42}]->(n2)," + newLine +
                "  (n0)-[:TYPE1 {bar:baz}]->(n1)," + newLine +
                "  (n1)-[:TYPE2]->(n2)," + newLine +
                "  (n2)-[:TYPE]->(n3);";

        assertEquals(expected, output.toString().trim());
    }

    @Test
    void testDumpByGraphForHuge() {
        //language=Cypher
        String expected =
                "CREATE (n0 {property:42.0})" + newLine +
                "CREATE (n1 {property:42.1337})" + newLine +
                "CREATE (n2 {property:42.0})" + newLine +
                "CREATE (n3 {property:42.0})" + newLine +
                "CREATE" + newLine +
                "  (n0)-[ {weight:42.0}]->(n1)," + newLine +
                "  (n0)-[ {weight:1337.42}]->(n2)," + newLine +
                "  (n1)-[ {weight:42.0}]->(n2)," + newLine +
                "  (n2)-[ {weight:42.0}]->(n3);";

        String output = dumpGraph();
        assertEquals(expected, output);
    }

    private String dumpGraph() {
        Graph graph = new StoreLoaderBuilder()
            .databaseService(db)
            .addNodeProperty(PropertyMapping.of("property", DefaultValue.of(42)))
            .addRelationshipProperty(PropertyMapping.of("property", DefaultValue.of(42)))
            .build()
            .graph();

        StringWriter output = new StringWriter();
        CypherExporter.export(new PrintWriter(output), graph);
        return output.toString().trim();
    }
}
