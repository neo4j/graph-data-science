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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.utils.progress.tasks.ImmutableProgress;

import static org.assertj.core.api.Assertions.assertThat;

class StructuredOutputHelperTest {

    @ParameterizedTest
    @CsvSource(value = {
        "4, 10, 10, [####~~~~~~]",
        "0, 10, 10, [~~~~~~~~~~]",
        "10, 10, 10, [##########]",
        "22, 100, 5, [#~~~~]",
        "0, 0, 5, [#####]"
    })
    void shouldComputeProgressBar(long progress, long volume, int progressBarLength, String expected) {
        assertThat(StructuredOutputHelper.progressBar(ImmutableProgress.of(progress, volume), progressBarLength)).isEqualTo(expected);
    }

    @Test
    void shouldComputeUnknownProgressBar() {
        assertThat(StructuredOutputHelper.progressBar(ImmutableProgress.of(1, -1), 10)).isEqualTo("[~~~~n/a~~~]");
    }

    @ParameterizedTest
    @CsvSource(value = {
        "42, 100, 42%",
        "0, 100, 0%",
        "100, 100, 100%",
        "1, 3, 33.33%",
        "0, 0, 100%"
    })
    void shouldComputeProgress(long progress, long volume, String expectedPercentage) {
        assertThat(StructuredOutputHelper.computeProgress(ImmutableProgress.of(progress, volume))).isEqualTo(expectedPercentage);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "foo, 3,'            |-- foo'",
        "bar, 0,|-- bar"
    })
    void shouldCreateTreeView(String description, int depth, String expectedTreeView) {
        assertThat(StructuredOutputHelper.treeViewDescription(description, depth)).isEqualTo(expectedTreeView);
    }
}
