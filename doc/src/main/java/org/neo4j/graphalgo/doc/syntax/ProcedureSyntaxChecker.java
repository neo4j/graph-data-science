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
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcedureSyntaxChecker extends Treeprocessor {

    private static final String STYLE_SELECTOR = "style";
    private static final String STYLE_SELECTOR_VALUE = "source";
    private static final String LANGUAGE_SELECTOR = "language";
    private static final String LANGUAGE_SELECTOR_VALUE = "cypher";
    private static final String CONTEXT_SELECTOR = "context";
    private static final String TABLE_CONTEXT_VALUE = ":table";
    public static final String RESULTS_TABLE_TITLE = "Results";
    public static final String RESULT_FIELD_TYPE_REGEX = "(: |:)[A-Za-z]+|([\\[\\]])";
    public static final String YIELD_KEYWORD = "YIELD";


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

    private static final String ROLE_SELECTOR = "role";


    private final Map<SyntaxMode, Class<?>> resultClasses;
    private final SoftAssertions syntaxAssertions;

    public ProcedureSyntaxChecker(
        Map<SyntaxMode, Class<?>> resultClasses,
        SoftAssertions syntaxAssertions
    ) {
        this.resultClasses = resultClasses;
        this.syntaxAssertions = syntaxAssertions;
    }

    @Override
    public Document process(Document document) {
        resultClasses.forEach((mode, resultClass) -> {
            var expectedResultFieldsFromCode = extractExpectedResultFields(resultClass);

            var allSyntaxSectionsForMode = document.findBy(Map.of(ROLE_SELECTOR, mode.mode()));

            assertThat(allSyntaxSectionsForMode)
                .as("There was an issue with `%s`", mode.mode())
                .hasSize(1)
                .allSatisfy(currentWorkingDocument -> {
                    assertYieldResultColumns(currentWorkingDocument, mode, expectedResultFieldsFromCode);
                    assertResultsTable(currentWorkingDocument, mode, expectedResultFieldsFromCode);
                });
        });

        return document;
    }

    private void assertResultsTable(
        StructuralNode currentWorkingDocument,
        SyntaxMode mode,
        Iterable<String> expectedResultFieldsFromCode
    ) {
        var resultTablesStream = currentWorkingDocument.findBy(Map.of(CONTEXT_SELECTOR, TABLE_CONTEXT_VALUE))
            .stream()
            .filter(node -> node.getTitle().equals(RESULTS_TABLE_TITLE));

        assertThat(resultTablesStream)
            .as("There is an issue finding the results table for `%s`", mode.mode())
            .hasSize(1)
            .allSatisfy(assertResultsTable(mode, expectedResultFieldsFromCode));
    }

    private void assertYieldResultColumns(
        StructuralNode currentWorkingDocument,
        SyntaxMode mode,
        Collection<String> expectedResultFieldsFromCode
    ) {
        var syntaxSectionContentStream = currentWorkingDocument.findBy(Map.of(
            STYLE_SELECTOR, STYLE_SELECTOR_VALUE,
            LANGUAGE_SELECTOR, LANGUAGE_SELECTOR_VALUE
        )).stream()
            .map(StructuralNode::getContent)
            .map(Object::toString);

        assertThat(syntaxSectionContentStream)
            .as("There is an issue finding the code block for `%s`", mode.mode())
            .hasSize(1)
            .allSatisfy(assertYieldResultColumns(mode, expectedResultFieldsFromCode));
    }

    private Consumer<String> assertYieldResultColumns(
        SyntaxMode mode,
        Collection<String> expectedResultFieldsFromCode
    ) {
        return syntaxSectionContent -> {
            var yieldResultColumns = extractDocResultFields(syntaxSectionContent);

            syntaxAssertions.assertThat(yieldResultColumns)
                .as("Asserting YIELD result columns for `%s`", mode.mode())
                .containsExactlyInAnyOrderElementsOf(expectedResultFieldsFromCode);
        };
    }

    private Consumer<StructuralNode> assertResultsTable(
        SyntaxMode mode,
        Iterable<String> expectedResultFieldsFromCode
    ) {
        return rawDocTable -> {

            assertThat(rawDocTable).isInstanceOf(Table.class);

            var docTable = (Table) rawDocTable;
            var actualResultTableFields = docTable
                .getBody()
                .stream()
                .map(row -> row
                    .getCells()
                    .get(0)) // Get the first column in the row --> corresponds to the return column names
                .map(Cell::getText)
                .collect(Collectors.toList());

            syntaxAssertions.assertThat(actualResultTableFields)
                .as("Asserting result table for `%s`", mode.mode())
                .containsExactlyInAnyOrderElementsOf(expectedResultFieldsFromCode);
        };
    }

    private Collection<String> extractDocResultFields(String syntaxCode) {
        var yield = syntaxCode.substring(syntaxCode.indexOf(YIELD_KEYWORD) + YIELD_KEYWORD.length()).trim();
        return Arrays.stream(yield.replaceAll(RESULT_FIELD_TYPE_REGEX, "").split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    }

    private Collection<String> extractExpectedResultFields(Class<?> resultClass) {
        return Arrays
            .stream(resultClass.getFields())
            // Deprecated fields shouldn't be in the documentation
            // Ignore static final fields
            .filter(field -> field.getAnnotation(Deprecated.class) == null
                             && !(Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())))
            .map(Field::getName)
            .collect(Collectors.toList());
    }
}
