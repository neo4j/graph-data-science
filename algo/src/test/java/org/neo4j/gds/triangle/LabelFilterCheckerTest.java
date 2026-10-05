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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.NodeLabel;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LabelFilterCheckerTest {

    @Test
    void shouldKeepPairWhenOnlyThirdNodeMatchesSingleLabel() {
        var checker = new LabelFilterChecker(List.of("A"), (node, label) -> node == 2);

        assertThat(checker.check(0, 1, 2)).isTrue();
        assertThat(checker.check(0, 1)).isTrue();
    }

    @Test
    void shouldKeepPairWhenThirdNodeCompletesTwoLabels() {
        var labels = Map.of(0L, NodeLabel.of("C"), 1L, NodeLabel.of("B"), 2L, NodeLabel.of("A"));
        var checker = new LabelFilterChecker(List.of("A", "B"), (node, label) -> labels.get(node).equals(label));

        assertThat(checker.check(0, 1, 2)).isTrue();
        assertThat(checker.check(0, 1)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
        "A, B, C, true",
        "A, C, B, true",
        "B, A, C, true",
        "B, C, A, true",
        "C, A, B, true",
        "C, B, A, true",
        "A, C, A, false",
        "A, A, C, false",
        "A, A, B, false",
        "B, C, C, false"
    })
    void shouldRequireAllThreeLabels(String firstLabel, String secondLabel, String thirdLabel, boolean expected) {
        var labels = Map.of(
            0L, NodeLabel.of(firstLabel),
            1L, NodeLabel.of(secondLabel),
            2L, NodeLabel.of(thirdLabel)
        );
        var checker = new LabelFilterChecker(List.of("A", "B", "C"), (node, label) -> labels.get(node).equals(label));

        assertThat(checker.check(0, 1, 2)).isEqualTo(expected);
    }
}
