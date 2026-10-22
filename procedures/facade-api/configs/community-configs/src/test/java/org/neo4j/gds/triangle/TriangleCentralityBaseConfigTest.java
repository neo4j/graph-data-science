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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TriangleCentralityBaseConfigTest {

    @Test
    void shouldUseUnlimitedDegreeByDefault() {
        var config = TriangleCentralityStreamConfig.of(CypherMapWrapper.empty());

        assertThat(config.maxDegree()).isEqualTo(Long.MAX_VALUE);
        assertThat(config.toParameters().maxDegree()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void shouldParseMaxDegree() {
        var config = TriangleCentralityStreamConfig.of(CypherMapWrapper.create(Map.of("maxDegree", 42L)));

        assertThat(config.maxDegree()).isEqualTo(42);
        assertThat(config.toParameters().maxDegree()).isEqualTo(42);
    }

    @Test
    void shouldParseLabelFilter() {
        var config = TriangleCentralityStreamConfig.of(
            CypherMapWrapper.create(Map.of("labelFilter", List.of("A", "B", "C")))
        );

        assertThat(config.labelFilter()).containsExactly("A", "B", "C");
        assertThat(config.toParameters().labelFilter()).containsExactly("A", "B", "C");
    }

    @Test
    void shouldRejectMoreThanThreeLabels() {
        assertThatThrownBy(() -> TriangleCentralityStreamConfig.of(
            CypherMapWrapper.create(Map.of("labelFilter", List.of("A", "B", "C", "D")))
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The provided 'labelFilter' list must only contain up to three elements.");
    }

    @Test
    void shouldRejectMaxDegreeBelowTwo() {
        assertThatThrownBy(() -> TriangleCentralityStreamConfig.of(
            CypherMapWrapper.create(Map.of("maxDegree", 1L))
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The 'maxDegree' parameter must be set to a value greater than 1.");
    }
}
