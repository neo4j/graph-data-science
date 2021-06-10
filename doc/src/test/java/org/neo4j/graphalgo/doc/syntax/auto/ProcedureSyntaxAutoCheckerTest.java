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
package org.neo4j.graphalgo.doc.syntax.auto;

import org.asciidoctor.Asciidoctor;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SoftAssertionsExtension.class)
class ProcedureSyntaxAutoCheckerTest {

    @Test
    void correctSyntaxSectionTest(SoftAssertions softAssertions) throws URISyntaxException {
        var asciidoctor = createAsciidoctor(softAssertions);
        var file = Paths.get(getClass().getClassLoader().getResource("include-with-syntax.adoc").toURI()).toFile();
        assertTrue(file.exists() && file.canRead());

        asciidoctor.loadFile(file, Collections.emptyMap());

        softAssertions.assertAll();
    }

    @Test
    void shouldFailOnMissingResultsTable() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass().getClassLoader().getResource("invalid-include-with-syntax-no-results-table.adoc").toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        assertThatThrownBy(() -> asciidoctor.loadFile(file, Map.of()))
            .hasMessageContaining("There is an issue finding the results table for `include-with-stream`")
            .hasMessageContaining("Expected size: 1 but was: 0");
    }

    @Test
    void shouldFailOnMoreThanOneResultsTable() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass().getClassLoader().getResource("invalid-include-with-syntax-two-results-tables.adoc").toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        assertThatThrownBy(() -> asciidoctor.loadFile(file, Map.of()))
            .hasMessageContaining("There is an issue finding the results table for `include-with-stream`")
            .hasMessageContaining("Expected size: 1 but was: 2");
    }

    @Test
    void shouldFailOnMissingCodeBlock() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass().getClassLoader().getResource("invalid-include-with-syntax-no-code-block.adoc").toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        assertThatThrownBy(() -> asciidoctor.loadFile(file, Map.of()))
            .hasMessageContaining("There is an issue finding the code block for `include-with-stream`")
            .hasMessageContaining("Expected size: 1 but was: 0");
    }

    @Test
    void shouldFailOnMoreThanOneCodeBlock() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass().getClassLoader().getResource("invalid-include-with-syntax-two-code-blocks.adoc").toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        assertThatThrownBy(() -> asciidoctor.loadFile(file, Map.of()))
            .hasMessageContaining("There is an issue finding the code block for `include-with-stream`")
            .hasMessageContaining("Expected size: 1 but was: 2");
    }

    @Test
    void shouldFailOnMissingYieldResultColumns() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass()
                .getClassLoader()
                .getResource("invalid-include-with-syntax-missing-yield-columns.adoc")
                .toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        asciidoctor.loadFile(file, Map.of());

        assertThat(syntaxAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> {
                assertThat(assertionError)
                    .hasMessageContaining("Asserting YIELD result columns for `include-with-stream`")
                    .hasMessageContaining("could not find the following elements:\n" +
                                          "  [\"communityId\", \"intermediateCommunityIds\"]");
            });
    }

    @Test
    void shouldFailOnExtraYieldResultColumns() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass()
                .getClassLoader()
                .getResource("invalid-include-with-syntax-extra-yield-columns.adoc")
                .toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        asciidoctor.loadFile(file, Map.of());

        assertThat(syntaxAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> {
                assertThat(assertionError)
                    .hasMessageContaining("Asserting YIELD result columns for `include-with-stream`")
                    .hasMessageContaining("the following elements were unexpected:\n" +
                                          "  [\"bogusResultColumn\"]");
            });
    }

    @Test
    void shouldFailOnMissingResultTableRows() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass()
                .getClassLoader()
                .getResource("invalid-include-with-syntax-missing-result-table-rows.adoc")
                .toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        asciidoctor.loadFile(file, Map.of());

        assertThat(syntaxAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> {
                assertThat(assertionError)
                    .hasMessageContaining("Asserting result table for `include-with-stream`")
                    .hasMessageContaining("could not find the following elements:\n" +
                                          "  [\"intermediateCommunityIds\"]");
            });
    }

    @Test
    void shouldFailOnExtraResultTableRows() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        var asciidoctor = createAsciidoctor(syntaxAssertions);
        var file = Paths
            .get(getClass()
                .getClassLoader()
                .getResource("invalid-include-with-syntax-extra-result-table-rows.adoc")
                .toURI())
            .toFile();
        assertTrue(file.exists() && file.canRead());

        asciidoctor.loadFile(file, Map.of());

        assertThat(syntaxAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> {
                assertThat(assertionError)
                    .hasMessageContaining("Asserting result table for `include-with-stream`")
                    .hasMessageContaining("the following elements were unexpected:\n" +
                                          "  [\"bogusResultColumn\"]");
            });
    }

    private Asciidoctor createAsciidoctor(SoftAssertions softAssertions) {
        var asciidoctor = Asciidoctor.Factory.create();
        asciidoctor.javaExtensionRegistry()
            .treeprocessor(
                new ProcedureSyntaxAutoChecker(
                    List.of(ProcedureSyntaxAutoChecker.SyntaxMode.STREAM),
                    softAssertions
                ));
        return asciidoctor;
    }
}
