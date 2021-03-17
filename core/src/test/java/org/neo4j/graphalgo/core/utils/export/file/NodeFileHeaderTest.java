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
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import static org.assertj.core.api.Assertions.assertThat;

class NodeFileHeaderTest {

    @Test
    void shouldBuildNodeFileHeader() {
        var headerLine = ":ID,foo:LONG,bar:DOUBLE,baz:LONG,neoId:LONG";

        var fileHeader = NodeFileHeader.builder()
            .withHeaderLine(headerLine)
            .withNodeLabels(new String[]{"A", "B", "C"})
            .build();

        assertThat(fileHeader).isNotNull();
        assertThat(fileHeader.nodeLabels()).containsExactlyInAnyOrder("A", "B", "C");
        assertThat(fileHeader.propertyMappings()).containsExactlyInAnyOrder(
            ImmutableHeaderProperty.of(1, "foo", ValueType.LONG),
            ImmutableHeaderProperty.of(2, "bar", ValueType.DOUBLE),
            ImmutableHeaderProperty.of(3, "baz", ValueType.LONG),
            ImmutableHeaderProperty.of(4, "neoId", ValueType.LONG)
        );
    }
}
