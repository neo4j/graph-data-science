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
package org.neo4j.gds.beta.pregel;

import org.assertj.core.api.SoftAssertions;
import org.neo4j.gds.TestGraph;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class TestSupport {

    private TestSupport() {}

    public static void assertLongValues(TestGraph graph, Function<Long, Long> actualValues, Map<String, Long> expectedValues) {
        var softly = new SoftAssertions();

        expectedValues.forEach((variable, expectedValue) -> {
            Long actualValue = actualValues.apply(graph.toMappedNodeId(variable));
            softly.assertThat(actualValue).withFailMessage(String.format(
                Locale.ENGLISH,
                "Values do not match for variable %s. Expected %s, got %s.",
                variable,
                expectedValue.toString(),
                actualValue.toString()
            )).isEqualTo(expectedValue);
        });

        softly.assertAll();
    }

    public static void assertDoubleValues(TestGraph graph, Function<Long, Double> actualValues, Map<String, Double> expectedValues, double delta) {
        expectedValues.forEach((variable, expectedValue) -> {
            Double actualValue = actualValues.apply(graph.toMappedNodeId(variable));
            assertEquals(
                expectedValue,
                actualValue,
                delta,
                String.format(
                    Locale.ENGLISH,
                    "Values do not match for variable %s. Expected %s, got %s.",
                    variable,
                    expectedValue.toString(),
                    actualValue.toString()
                ));
        });
    }
}
