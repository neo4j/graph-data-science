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
package org.neo4j.graphalgo.doc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CypherAsciidocTableConverterTest {

    private static final String CYPHER = "+----------------------------------------+\n" +
                                         "| Person1 | Person2 | similarity         |\n" +
                                         "+----------------------------------------+\n" +
                                         "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                         "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                         "| \"Alice\" | \"Bob\"   | 0.6666666666666666 |\n" +
                                         "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                         "| \"Bob\"   | \"Dave\"  | 0.6666666666666666 |\n" +
                                         "| \"Dave\"  | \"Bob\"   | 0.6666666666666666 |\n" +
                                         "| \"Alice\" | \"Carol\" | 0.3333333333333333 |\n" +
                                         "| \"Carol\" | \"Alice\" | 0.3333333333333333 |\n" +
                                         "| \"Carol\" | \"Dave\"  | 0.3333333333333333 |\n" +
                                         "| \"Dave\"  | \"Carol\" | 0.3333333333333333 |\n" +
                                         "+----------------------------------------+\n" +
                                         "10 rows\n";

    private static final String ASCIIDOC = "| Person1 | Person2 | similarity\n" +
                                           "| \"Alice\" | \"Dave\"  | 1.0\n" +
                                           "| \"Dave\"  | \"Alice\" | 1.0\n" +
                                           "| \"Alice\" | \"Bob\"   | 0.6666666666666666\n" +
                                           "| \"Bob\"   | \"Alice\" | 0.6666666666666666\n" +
                                           "| \"Bob\"   | \"Dave\"  | 0.6666666666666666\n" +
                                           "| \"Dave\"  | \"Bob\"   | 0.6666666666666666\n" +
                                           "| \"Alice\" | \"Carol\" | 0.3333333333333333\n" +
                                           "| \"Carol\" | \"Alice\" | 0.3333333333333333\n" +
                                           "| \"Carol\" | \"Dave\"  | 0.3333333333333333\n" +
                                           "| \"Dave\"  | \"Carol\" | 0.3333333333333333\n" +
                                           "3+|10 rows\n";

    @Test
    void shouldConvertCypherTableToAsciidoc() {
        String actual = CypherAsciidocTableConverter.asciidoc(CYPHER);
        assertEquals(ASCIIDOC, actual);
    }

    @Test
    void shouldConvertAsciidocTableToCypher() {
        String actual = CypherAsciidocTableConverter.cypher(ASCIIDOC);
        assertEquals(CYPHER, actual);
    }

}
