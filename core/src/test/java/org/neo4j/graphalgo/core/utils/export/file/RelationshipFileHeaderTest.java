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
package org.neo4j.graphalgo.core.utils.export.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor.END_ID_COLUMN_NAME;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvRelationshipVisitor.START_ID_COLUMN_NAME;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class RelationshipFileHeaderTest {

    @Test
    void shouldBuildRelationshipFileHeader() {
        var headerLine = ":START_ID,:END_ID,foo:LONG,bar:DOUBLE,baz:LONG";

        var fileHeader = RelationshipFileHeader.builder()
            .withHeaderLine(headerLine)
            .withRelationshipType("REL")
            .build();

        assertThat(fileHeader).isNotNull();
        assertThat(fileHeader.relationshipType()).isEqualTo("REL");
        assertThat(fileHeader.propertyMappings()).containsExactlyInAnyOrder(
            ImmutableHeaderProperty.of(2, "foo", ValueType.LONG),
            ImmutableHeaderProperty.of(3, "bar", ValueType.DOUBLE),
            ImmutableHeaderProperty.of(4, "baz", ValueType.LONG)
        );
    }


    @ParameterizedTest
    @ValueSource(strings = {
        ":END_ID,foo:LONG,bar:DOUBLE",
        "",
        ":END_ID,:START_ID"
    })    void shouldFailIfStartIdColumnIsMissing(String headerLine) {
        assertThatThrownBy(() -> RelationshipFileHeader.builder()
            .withHeaderLine(headerLine)
            .withRelationshipType("REL")
            .build()
        )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("First column")
            .hasMessageContaining("must be " + START_ID_COLUMN_NAME);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        ":START_ID,foo:LONG,bar:DOUBLE",
        ":START_ID,foo:LONG,:END_ID,bar:DOUBLE",
        ":START_ID"
    })
    void shouldFailIfEndIdColumnIsMissing(String headerLine) {
        assertThatThrownBy(() -> RelationshipFileHeader.builder()
            .withHeaderLine(headerLine)
            .withRelationshipType("REL")
            .build()
        )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Second column")
            .hasMessageContaining("must be " + END_ID_COLUMN_NAME);
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo::Bar", ":BAR", "foo:", ":foo:BAR", ":"})
    void shouldFailOnMalformedProperties(String propertyHeaderString) {
        var headerString = formatWithLocale("%s,%s,%s", START_ID_COLUMN_NAME, END_ID_COLUMN_NAME, propertyHeaderString);
        assertThatThrownBy(() -> RelationshipFileHeader.builder()
            .withHeaderLine(headerString)
            .withRelationshipType("REL")
            .build()
        )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not have expected format <string>:<string>");
    }
}
