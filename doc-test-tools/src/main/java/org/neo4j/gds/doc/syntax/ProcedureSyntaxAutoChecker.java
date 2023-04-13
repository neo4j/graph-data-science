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

import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.extension.Postprocessor;
import org.assertj.core.api.SoftAssertions;
import org.neo4j.gds.annotation.CustomProcedure;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ProcedureSyntaxAutoChecker extends Postprocessor {

    private static final String YIELD_FIELD_SEPARATOR = ",";
    private static final String YIELD_NAME_DATA_TYPE_SEPARATOR = ":";

    private static final String STYLE_SELECTOR = "style";
    private static final String STYLE_SELECTOR_VALUE = "source";
    private static final String LANGUAGE_SELECTOR = "language";
    private static final String LANGUAGE_SELECTOR_VALUE = "cypher";
    private static final String CONTEXT_SELECTOR = "context";
    private static final String TABLE_CONTEXT_VALUE = ":table";
    private static final String RESULTS_TABLE_TITLE = "Results";
    private static final String PARAMETERS_TABLE_TITLE = "Parameters";
    private static final String YIELD_KEYWORD = "YIELD";

    private static final String ROLE_SELECTOR = "role";


    private final Iterable<SyntaxModeMeta> syntaxModesPerPage;
    private final SoftAssertions syntaxAssertions;
    private final ProcedureLookup procedureLookup;

    public ProcedureSyntaxAutoChecker(
        Iterable<SyntaxModeMeta> syntaxModesPerPage,
        SoftAssertions syntaxAssertions,
        ProcedureLookup procedureLookup
    ) {
        this.syntaxModesPerPage = syntaxModesPerPage;
        this.syntaxAssertions = syntaxAssertions;
        this.procedureLookup = procedureLookup;
    }

    @Override
    public String process(Document document, String output) {
        syntaxModesPerPage.forEach(mode -> {

            var allSyntaxSectionsForMode = document.findBy(Map.of(ROLE_SELECTOR,  mode.syntaxMode().mode()));

            assertThat(allSyntaxSectionsForMode)
                .as("There was an issue with `%s`", mode.syntaxMode().mode())
                .hasSize( mode.sectionsOnPage());

            var currentSyntaxSection = allSyntaxSectionsForMode.get(0);
            var codeSnippet = extractSyntaxCodeSnippet(currentSyntaxSection,  mode.syntaxMode());
            var procedureName = ProcedureNameExtractor.findProcedureName(codeSnippet);

            var documentedArguments = ProcedureArgumentsExtractor.findArguments(codeSnippet);
            var expectedArguments = procedureLookup.findArgumentNames(procedureName);

            if (mode.syntaxMode().hasParameters) {
                syntaxAssertions.assertThat(documentedArguments)
                    .as("Asserting procedure arguments for `%s`", mode.syntaxMode().mode())
                    .containsExactlyInAnyOrderElementsOf(expectedArguments);

                assertTableValues(currentSyntaxSection, mode.syntaxMode(), PARAMETERS_TABLE_TITLE, expectedArguments);
            }

            // YIELD fields
            var resultClass = procedureLookup.findResultType(procedureName);
            var expectedResultFieldsFromCode = extractExpectedResultFields(resultClass);

            var yieldResultColumns = extractDocResultFields(codeSnippet);

            syntaxAssertions.assertThat(yieldResultColumns)
                .as("Asserting YIELD result columns for `%s`",  mode.syntaxMode().mode())
                .containsExactlyInAnyOrderElementsOf(expectedResultFieldsFromCode);

            assertTableValues(currentSyntaxSection, mode.syntaxMode(), RESULTS_TABLE_TITLE, expectedResultFieldsFromCode);
        });

        return output;
    }

    private String extractSyntaxCodeSnippet(
        StructuralNode currentWorkingDocument,
        SyntaxMode mode
    ) {
        var syntaxSectionContentStream = currentWorkingDocument.findBy(Map.of(
            STYLE_SELECTOR, STYLE_SELECTOR_VALUE,
            LANGUAGE_SELECTOR, LANGUAGE_SELECTOR_VALUE
        )).stream()
            .map(StructuralNode::getContent)
            .map(Object::toString)
            .collect(Collectors.toList());

        assertThat(syntaxSectionContentStream)
            .as("There is an issue finding the code block for `%s`", mode.mode())
            .hasSize(1);

        return syntaxSectionContentStream.get(0);
    }

    private void assertTableValues(
        StructuralNode currentWorkingDocument,
        SyntaxMode mode,
        String tableTitle,
        Iterable<String> expectedValues
    ) {
        var resultTablesStream = currentWorkingDocument.findBy(Map.of(CONTEXT_SELECTOR, TABLE_CONTEXT_VALUE))
            .stream()
            .filter(node -> node.getTitle().equals(tableTitle));

        assertThat(resultTablesStream)
            .as("There is an issue finding the `%s` table for `%s`", tableTitle, mode.mode())
            .hasSize(1)
            .allSatisfy(assertTableValues(mode, expectedValues, tableTitle));
    }

    private Consumer<StructuralNode> assertTableValues(
        SyntaxMode mode,
        Iterable<String> expectedValues,
        String tableTitle
    ) {
        return rawDocTable -> {

            assertThat(rawDocTable).isInstanceOf(Table.class);

            var docTable = (Table) rawDocTable;
            var documentedValues = docTable
                .getBody()
                .stream()
                .map(row -> row
                    .getCells()
                    .get(0)) // Get the first column in the row --> corresponds to the return column names
                .map(Cell::getText)
                // remove any potential links in the names
                .map(name -> name.replaceAll("<a.*\">|<\\/a>", ""))
                // as java identifier cannot contain white spaces, remove anything after the first space such as footnote:
                .map(name -> name.split("\\s+")[0])
                .collect(Collectors.toList());

            syntaxAssertions.assertThat(documentedValues)
                .as("Asserting `%s` table for `%s`", tableTitle, mode.mode())
                .containsExactlyInAnyOrderElementsOf(expectedValues);
        };
    }

    private static Iterable<String> extractDocResultFields(String syntaxCode) {
        var yield = syntaxCode.substring(syntaxCode.indexOf(YIELD_KEYWORD) + YIELD_KEYWORD.length()).trim();
        return Arrays.stream(yield.split(YIELD_FIELD_SEPARATOR))
            .map(yieldField -> yieldField.split(YIELD_NAME_DATA_TYPE_SEPARATOR)[0].trim())
            .collect(Collectors.toList());
    }

    private static Collection<String> extractExpectedResultFields(Class<?> resultClass) {
        return findResultFields(resultClass)
            .map(Member::getName)
            .collect(Collectors.toList());
    }

    private static Stream<? extends Member> findResultFields(Class<?> resultClass) {
        return resultClass.isInterface()
            ? resultFieldsFromInterfaceMethods(resultClass)
            : resultFieldsFromClassFields(resultClass);
    }

    private static Stream<Method> resultFieldsFromInterfaceMethods(Class<?> resultClass) {
        return Arrays
            .stream(resultClass.getDeclaredMethods())
            .filter(ProcedureSyntaxAutoChecker::includeMethodInResult);
    }

    private static Stream<Field> resultFieldsFromClassFields(Class<?> resultClass) {
        return Arrays
            .stream(resultClass.getFields())
            .filter(ProcedureSyntaxAutoChecker::includeFieldInResult);
    }

    private static boolean includeMethodInResult(AnnotatedElement method) {
        return method.isAnnotationPresent(CustomProcedure.ResultField.class);
    }

    private static boolean includeFieldInResult(Field field) {
        // Ignore static fields
        return !Modifier.isStatic(field.getModifiers());
    }
}
