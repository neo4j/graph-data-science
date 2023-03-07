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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

@GdlExtension
class NodeFilterParserTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {longProperty: 1, floatProperty: 15.5})" +
        ", (b:A {longProperty: 2, floatProperty: 23})" +
        ", (c:A {longProperty: 3, floatProperty: 18.3})" +
        ", (d:B)";

    @Inject
    GraphStore graphStore;

    @ParameterizedTest
    @ValueSource(
        strings = {
            "n.floatProperty > 10",
            "n.floatProperty >= 11",
            "n.floatProperty < 12",
            "n.floatProperty <= 19",
            "n.floatProperty = 1121",
            "n.longProperty > 11.0",
            "n.longProperty >= 3.14",
            "n.longProperty < 19.6",
            "n.longProperty <= 19.6",
            "n.longProperty = 21.42",
            "n.longProperty <> 2.0",
        }
    )
    void shouldFailOnIncompatiblePropertyAndValue(String nodeFilter) {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> NodeFilterParser.parseAndValidate(graphStore, nodeFilter))
            .withRootCauseInstanceOf(SemanticErrors.class)
            .havingRootCause()
            .withMessageContaining("Semantic errors while parsing expression")
            .withMessageContaining("Incompatible types");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
            "n.longProperty > 10",
            "n.longProperty >= 11",
            "n.longProperty < 12",
            "n.longProperty <= 19",
            "n.longProperty = 1121",
            "n.floatProperty > 11.0",
            "n.floatProperty >= 3.14",
            "n.floatProperty < 19.6",
            "n.floatProperty <= 19.6",
            "n.floatProperty = 21.42",
            "n.floatProperty <> 2.0",
        }
    )
    void shouldNotFailOnCompatiblePropertyAndValue(String nodeFilter) {
        assertThatNoException()
            .isThrownBy(() -> NodeFilterParser.parseAndValidate(graphStore, nodeFilter));
    }

}
