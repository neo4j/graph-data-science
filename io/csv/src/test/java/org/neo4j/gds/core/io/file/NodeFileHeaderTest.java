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
package org.neo4j.gds.core.io.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.nodeproperties.ValueType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.core.io.file.csv.CsvNodeVisitor.ID_COLUMN_NAME;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class NodeFileHeaderTest {

    @Test
    void shouldBuildNodeFileHeader() {
        var headerLine = ":ID,foo:long,bar:double,baz:long";

        var fileHeader = NodeFileHeader.of(headerLine, new String[]{"A", "B", "C"});

        assertThat(fileHeader).isNotNull();
        assertThat(fileHeader.nodeLabels()).containsExactlyInAnyOrder("A", "B", "C");
        assertThat(fileHeader.propertyMappings()).containsExactlyInAnyOrder(
            ImmutableHeaderProperty.of(1, "foo", ValueType.LONG),
            ImmutableHeaderProperty.of(2, "bar", ValueType.DOUBLE),
            ImmutableHeaderProperty.of(3, "baz", ValueType.LONG)
        );
    }

    @Test
    void shouldFailIfIdColumnIsMissing() {
        var headerLine = "foo:long,bar:double";

        assertThatThrownBy(() -> NodeFileHeader.of(headerLine, new String[]{"A"}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("First column")
            .hasMessageContaining("must be " + ID_COLUMN_NAME);
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo::Bar", ":BAR", "foo:", ":foo:BAR", ":"})
    void shouldFailOnMalformedProperties(String propertyHeaderString) {
        var headerString = formatWithLocale("%s,%s", ID_COLUMN_NAME, propertyHeaderString);
        assertThatThrownBy(() -> NodeFileHeader.of(headerString, new String[]{"A"}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not have expected format <string>:<string>");
    }
}
