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
package org.neo4j.gds.applications.graphstorecatalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.GraphName;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class GraphNameValidationServiceTest {
    @Test
    void shouldEnsureGraphNameValid() {
        var graphNameValidationService = new GraphNameValidationService();

        var validatedGraphName = graphNameValidationService.validateSingleOrList("a graph name");

        assertThat(validatedGraphName).containsExactly(GraphName.parse("a graph name"));
    }

    @Test
    void shouldEnsureGraphNamesValid() {
        var graphNameValidationService = new GraphNameValidationService();

        var validatedGraphNames = graphNameValidationService.validateSingleOrList(List.of(
            "a graph name",
            "another graph name"
        ));

        assertThat(validatedGraphNames).containsExactly(
            GraphName.parse("a graph name"),
            GraphName.parse("another graph name")
        );
    }

    @Test
    void shouldTrimWhenValidating() {
        var graphNameValidationService = new GraphNameValidationService();

        assertThat(graphNameValidationService.validateSingleOrList("   a graph name   "))
            .containsExactly(GraphName.parse("a graph name"));
        assertThat(graphNameValidationService.validateSingleOrList(List.of("   a graph name", "another graph name   ")))
            .containsExactly(
                GraphName.parse("a graph name"),
                GraphName.parse("another graph name")
            );
    }

    @Test
    void shouldFailValidationWhenGraphNameEmpty() {
        var graphNameValidationService = new GraphNameValidationService();

        try {
            graphNameValidationService.validateSingleOrList("");
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was ``");
        }

        try {
            graphNameValidationService.validateSingleOrList("   ");
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was `   `");
        }

        try {
            graphNameValidationService.validateSingleOrList(List.<Object>of("a graph name", "", "another graph name"));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was ``");
        }
    }

    // something found in another test
    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("invalidGraphNames")
    void shouldCatchInvalidGraphNames(String graphName) {
        var service = new GraphNameValidationService();

        try {
            service.validateSingleOrList(graphName);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was `" + graphName + "`");
        }
    }

    @Test
    void shouldFailValidationWhenGraphNameNotStringOrListOfString() {
        var graphNameValidationService = new GraphNameValidationService();

        try {
            graphNameValidationService.validateSingleOrList(42);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Type mismatch: expected String but was Integer.");
        }

        try {
            graphNameValidationService.validateSingleOrList(List.<Object>of("a graph name", 42, "another graph name"));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Type mismatch at index 1: expected String but was Integer.");
        }
    }

    @Test
    void shouldFailOnNullElements() {
        var graphNameValidationService = new GraphNameValidationService();

        try {
            graphNameValidationService.validateSingleOrList(asList("a graph name", null, "another graph name"));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was `null`");
        }
    }

    @Test
    void shouldValidateNulls() {
        var service = new GraphNameValidationService();

        assertThat(service.validatePossibleNull("some graph"))
            .isPresent()
            .hasValue(GraphName.parse("some graph"));

        assertThat(service.validatePossibleNull(null)).isEmpty();
    }

    @Test
    void shouldValidate() {
        var service = new GraphNameValidationService();

        assertThat(service.validate("some graph"))
            .isEqualTo(GraphName.parse("some graph"));
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("invalidGraphNames")
    void shouldCatchInvalidGraphNamesWhenValidating(String graphName) {
        var service = new GraphNameValidationService();

        try {
            service.validate(graphName);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was `" + graphName + "`");
        }
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("invalidGraphNames")
    void shouldCatchInvalidGraphNamesWhenValidatingStrictly(String graphName) {
        var service = new GraphNameValidationService();

        try {
            service.validateStrictly(graphName);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("`graphName` can not be null or blank, but it was `" + graphName + "`");
        }
    }

    @ParameterizedTest(name = "Invalid Graph Name: `{0}`")
    @MethodSource("strictlyInvalidGraphNames")
    void shouldRejectLeadingAndTrailingWhitespaceWhenStrictValidating(String graphName) {
        var service = new GraphNameValidationService();

        try {
            service.validateStrictly(graphName);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo(
                "`graphName` must not end or begin with whitespace characters, but got `" + graphName + "`.");
        }
    }

    private static Stream<String> invalidGraphNames() {
        return Stream.of("", "   ", "           ", "\r\n\t", null);
    }

    private static Stream<String> strictlyInvalidGraphNames() {
        return Stream.of("   no leading whitespace please", "no trailing whitespace please   ");
    }
}
