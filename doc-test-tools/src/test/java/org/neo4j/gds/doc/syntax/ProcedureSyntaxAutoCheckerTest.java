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
package org.neo4j.gds.doc.syntax;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.OptionsBuilder;
import org.asciidoctor.SafeMode;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STREAM;

@ExtendWith(SoftAssertionsExtension.class)
class ProcedureSyntaxAutoCheckerTest {

    @TempDir
    private File outputDirectory;
    private OptionsBuilder options;

    private static final String newLine = System.lineSeparator();

    @InjectSoftAssertions
    private SoftAssertions softAssertions;

    @BeforeEach
    void setUp() {
        // By default we are forced to use relative path which we don't want.
        options = OptionsBuilder.options()
            .toDir(outputDirectory) // Make sure we don't write anything in the project.
            .safe(SafeMode.UNSAFE);

    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"include-with-syntax.adoc", "include-with-syntax-parameter-with-link.adoc"})
    void correctSyntaxSectionTest(String positiveResource) throws URISyntaxException {
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths.get(getClass().getClassLoader().getResource(positiveResource).toURI()).toFile();
            assertTrue(file.exists() && file.canRead());

            asciidoctor.convertFile(file, options);
        }
    }

    @Test
    void shouldFailOnMissingResultsTable() throws URISyntaxException {
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-no-results-table.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            assertThatThrownBy(() -> asciidoctor.convertFile(file, options))
                .hasMessageContaining("There is an issue finding the `Results` table for `include-with-stream`")
                .hasMessageContaining("Expected size: 1 but was: 0");
        }
    }

    @Test
    void shouldFailOnMoreThanOneResultsTable() throws URISyntaxException {
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-two-results-tables.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            assertThatThrownBy(() -> asciidoctor.convertFile(file, options))
                .hasMessageContaining("There is an issue finding the `Results` table for `include-with-stream`")
                .hasMessageContaining("Expected size: 1 but was: 2");
        }
    }

    @Test
    void shouldFailOnMissingCodeBlock() throws URISyntaxException {
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass().getClassLoader().getResource("invalid-include-with-syntax-no-code-block.adoc").toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            assertThatThrownBy(() -> asciidoctor.convertFile(file, options))
                .hasMessageContaining("There is an issue finding the code block for `include-with-stream`")
                .hasMessageContaining("Expected size: 1 but was: 0");
        }
    }

    @Test
    void shouldFailOnMoreThanOneCodeBlock() throws URISyntaxException {
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-two-code-blocks.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            assertThatThrownBy(() -> asciidoctor.convertFile(file, options))
                .hasMessageContaining("There is an issue finding the code block for `include-with-stream`")
                .hasMessageContaining("Expected size: 1 but was: 2");
        }
    }

    @Test
    void shouldFailOnMissingYieldResultColumns() throws URISyntaxException {
        var softAssertions = new SoftAssertions();
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-missing-yield-columns.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            asciidoctor.convertFile(file, options);
        }

        assertThat(softAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> assertThat(assertionError)
                .hasMessageContaining("Asserting YIELD result columns for `include-with-stream`")
                .hasMessageContaining("could not find the following elements:" + newLine +
                                      "  [\"communityId\", \"intermediateCommunityIds\"]"));
    }

    @Test
    void shouldFailOnExtraYieldResultColumns() throws URISyntaxException {
        var syntaxAssertions = new SoftAssertions();
        try (var asciidoctor = createAsciidoctor(syntaxAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-extra-yield-columns.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            asciidoctor.convertFile(file, options);
        }

        assertThat(syntaxAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> assertThat(assertionError)
                .hasMessageContaining("Asserting YIELD result columns for `include-with-stream`")
                .hasMessageContaining("the following elements were unexpected:" + newLine +
                                      "  [\"bogusResultColumn\"]"));
    }

    @Test
    void shouldFailOnMissingResultTableRows() throws URISyntaxException {
        var softAssertions = new SoftAssertions();
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-missing-result-table-rows.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            asciidoctor.convertFile(file, options);
        }

        assertThat(softAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> assertThat(assertionError)
                .hasMessageContaining("Asserting `Results` table for `include-with-stream`")
                .hasMessageContaining("could not find the following elements:" + newLine +
                                      "  [\"intermediateCommunityIds\"]"));
    }

    @Test
    void shouldFailOnExtraResultTableRows() throws URISyntaxException {
        var softAssertions = new SoftAssertions();
        try (var asciidoctor = createAsciidoctor(softAssertions)) {
            var file = Paths
                .get(getClass()
                    .getClassLoader()
                    .getResource("invalid-include-with-syntax-extra-result-table-rows.adoc")
                    .toURI())
                .toFile();
            assertTrue(file.exists() && file.canRead());

            asciidoctor.convertFile(file, options);
        }

        assertThat(softAssertions.assertionErrorsCollected())
            .hasSize(1)
            .allSatisfy(assertionError -> assertThat(assertionError)
                .hasMessageContaining("Asserting `Results` table for `include-with-stream`")
                .hasMessageContaining("the following elements were unexpected:" + newLine +
                                      "  [\"bogusResultColumn\"]"));
    }

    private Asciidoctor createAsciidoctor(SoftAssertions softAssertions) {
        var asciidoctor = Asciidoctor.Factory.create();
        asciidoctor.javaExtensionRegistry()
            .postprocessor(
                new ProcedureSyntaxAutoChecker(
                    List.of(SyntaxModeMeta.of(STREAM)),
                    softAssertions,
                    ProcedureLookup.forPackages(List.of("org.neo4j.gds"))
                ));
        return asciidoctor;
    }
}
