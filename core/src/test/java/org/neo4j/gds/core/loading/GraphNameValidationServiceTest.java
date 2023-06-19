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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class GraphNameValidationServiceTest {
    @Test
    void shouldEnsureGraphNameValid() {
        var graphNameValidationService = new GraphNameValidationService();

        var validatedGraphName = graphNameValidationService.validateSingleOrList("a graph name");

        assertThat(validatedGraphName).containsExactly("a graph name");
    }

    @Test
    void shouldEnsureGraphNamesValid() {
        var graphNameValidationService = new GraphNameValidationService();

        var validatedGraphNames = graphNameValidationService.validateSingleOrList(List.of("a graph name", "another graph name"));

        assertThat(validatedGraphNames).containsExactly("a graph name", "another graph name");
    }

    @Test
    void shouldTrimWhenValidating() {
        var graphNameValidationService = new GraphNameValidationService();

        assertThat(graphNameValidationService.validateSingleOrList("   a graph name   "))
            .containsExactly("a graph name");
        assertThat(graphNameValidationService.validateSingleOrList(List.of("   a graph name", "another graph name   ")))
            .containsExactly("a graph name", "another graph name");
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

    /**
     * If not here then it would fail later anyway
     */
    @Test
    void shouldFailOnNulls() {
        var graphNameValidationService = new GraphNameValidationService();

        try {
            graphNameValidationService.validateSingleOrList(null);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Type mismatch: expected String but was null.");
        }

        try {
            graphNameValidationService.validateSingleOrList(asList("a graph name", null, "another graph name"));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Type mismatch at index 1: expected String but was null.");
        }
    }
}
