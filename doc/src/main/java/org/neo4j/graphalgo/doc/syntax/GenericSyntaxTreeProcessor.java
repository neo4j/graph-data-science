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
package org.neo4j.graphalgo.doc.syntax;

import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.extension.Treeprocessor;
import org.assertj.core.api.SoftAssertions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class GenericSyntaxTreeProcessor extends Treeprocessor {

    public enum SyntaxMode {
        STATS("include-with-stats"),
        STREAM("include-with-stream"),
        MUTATE("include-with-mutate"),
        WRITE("include-with-write"),
        TRAIN("include-with-train");

        private final String mode;

        SyntaxMode(String mode) {
            this.mode = mode;
        }

        String mode() {
            return mode;
        }
    }

    private final Map<SyntaxMode, Class<?>> resultClasses;
    private final SoftAssertions softAssertions;

    public GenericSyntaxTreeProcessor(
        Map<SyntaxMode, Class<?>> resultClasses,
        SoftAssertions softAssertions
    ) {
        this.resultClasses = resultClasses;
        this.softAssertions = softAssertions;
    }

    @Override
    public Document process(Document document) {
        resultClasses.forEach((mode, resultClass) -> {
            var expected = extractActualResultFields(resultClass);

            List<StructuralNode> nodes = document.findBy(Map.of("role", mode.mode()));

            assertThat(nodes)
                .as("There is an issue with `%s`", mode.mode())
                .hasSize(1);

            var currentWorkingDocument = nodes.get(0);

            var syntaxSectionContent = currentWorkingDocument
                .findBy(Map.of("style", "source", "language", "cypher"))
                .get(0)
                .getContent()
                .toString();
            var yieldResultColumns = extractDocResultFields(syntaxSectionContent);

            softAssertions.assertThat(yieldResultColumns)
                .as("Asserting YIELD result columns for `%s`", mode.mode())
                .containsExactlyInAnyOrderElementsOf(expected);

            var maybeResultsTable = currentWorkingDocument
                .findBy(Map.of("context", ":table")).stream().filter(node -> node.getTitle().equals("Results")).collect(
                Collectors.toList());

            assertThat(maybeResultsTable)
                .as("There is an issue finding the results table for `%s`", mode.mode())
                .hasSize(1);
            assertThat(maybeResultsTable.get(0)).isInstanceOf(Table.class);

            var docTable = (Table) maybeResultsTable.get(0);
            var actualResultTableFields = docTable
                .getBody()
                .stream()
                .map(row -> row
                    .getCells()
                    .get(0)) // Get the first column in the row --> corresponds to the return column names
                .map(Cell::getText)
                .collect(Collectors.toList());

            softAssertions.assertThat(actualResultTableFields)
                .as("Asserting result table for `%s`", mode.mode())
                .containsExactlyInAnyOrderElementsOf(expected);
        });
        return document;
    }

    Collection<String> extractDocResultFields(String syntax) {
        var yield = syntax.substring(syntax.indexOf("YIELD") + 5).trim();
        return Arrays.stream(yield.replaceAll("(: |:)[A-Za-z]+|([\\[\\]])", "").split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    }

    private Collection<String> extractActualResultFields(Class<?> resultClass) {
        return Arrays
            .stream(resultClass.getFields())
            // Deprecated fields shouldn't be in the documentation?!
            // Ignore static final fields
            .filter(field -> field.getAnnotation(Deprecated.class) == null
                             && !(Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())))
            .map(Field::getName)
            .collect(Collectors.toList());
    }
}
