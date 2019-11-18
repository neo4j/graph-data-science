/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.proc;

import com.google.common.io.Resources;
import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.lang.model.SourceVersion;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.truth.Truth.assertAbout;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaFileObjects.forSourceLines;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;
import static java.nio.charset.StandardCharsets.UTF_8;

@EnableRuleMigrationSupport
class ConfigurationProcessorTest {

    @Rule
    final CompilationRule compilationRule = new CompilationRule();

    @ParameterizedTest
    @ValueSource(strings = {
        "EmptyClass",
        "FieldTypes",
        "DefaultValues",
        "Inheritance",
        "Ignores",
        "NamingConflict",
        "KeyRenames",
        "Parameters",
        "ParametersOnly",
        "Conversions"
    })
    void goodTest(String className) {
        assertAbout(javaSource())
            .that(forResource(String.format("good/%s.java", className)))
            .processedWith(new ConfigurationProcessor())
            .compilesWithoutError()
            .and()
            .generatesSources(loadExpectedFile(String.format("expected/%s.java", className)));
    }

    @Test
    void baseClassMustBeAnInterface() {
        runBadTest(
            "BaseClassIsNotAnInterface",
            e(
                "The annotated configuration must be an interface.",
                6,
                17
            )
        );
    }

    @Test
    void failOnUnsupportedMethods() {
        runBadTest(
            "InvalidMethods",
            e("Unsupported return type: char", 10, 10),
            e("Unsupported return type: void", 12, 10),
            e("Unsupported return type: int[]", 14, 11),
            e("Method may not have any parameters", 16, 12),
            e("Method may not have any type parameters", 18, 11),
            e("Method may not have any type parameters", 20, 24),
            e("Method may not declare any exceptions to be thrown", 22, 9),
            e("Method may not declare any exceptions to be thrown", 24, 12)
        );
    }

    @Test
    void emptyKeyIsNotAllowed() {
        runBadTest(
            "EmptyKey",
            e("The key must not be empty", 9, 9),
            e("The key must not be empty", 12, 9)
        );
    }

    @Test
    void invalidAnnotationCombinations() {
        runBadTest(
            "InvalidAnnotationCombinations",
            e("The `@Parameter` annotation cannot be used together with the `@Key` annotation", 12, 9)
        );
    }

    @Test
    void invalidConversionsClassTargets() {
        runBadTest(
            "InvalidConversionsClasses",
            e("Empty conversion method is not allowed", 8, 5),
            e("Multiple possible candidates found: [multipleOverloads(java.lang.String), multipleOverloads(long)]", 11, 5),
            e("Method is ambiguous and a possible candidate for [multipleOverloads]", 14, 16),
            e("Method is ambiguous and a possible candidate for [multipleOverloads]", 18, 16),
            e("[bad.class.does.not.exist#foo] is not a valid fully qualified method name: The class [bad.class.does.not.exist] cannot be found", 22, 5),
            e("No suitable method found that matches [methodDoesNotExist]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 25, 5),
            e("No suitable method found that matches [bad.InvalidConversionsClasses#methodDoesNotExist]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 28, 5),
            e("[bad.InvalidConversionsClasses#] is not a valid fully qualified method name: it must start with a fully qualified class name followed by a '#' and then the method name", 31, 5)
        );
    }

    @Test
    void invalidConversionsMethodTargets() {
        runBadTest(
            "InvalidConversionsMethods",
            e("No suitable method found that matches [nonStatic]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 8, 5),
            e("Must be static", 12, 17),
            e("No suitable method found that matches [generic]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 17, 5),
            e("May not be generic", 20, 18),
            e("No suitable method found that matches [declaresThrows]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 25, 5),
            e("May not declare any exceptions", 28, 16),
            e("No suitable method found that matches [noParameters]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 33, 5),
            e("May only accept one parameter", 36, 16),
            e("No suitable method found that matches [multipleParameters]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 41, 5),
            e("May only accept one parameter", 44, 16),
            e("No suitable method found that matches [invalidReturnType1]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 49, 5),
            e("Must return a type that is assignable to int", 52, 19),
            e("No suitable method found that matches [invalidReturnType2]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 57, 5),
            e("Must return a type that is assignable to int", 60, 17),
            e("No suitable method found that matches [invalidReturnType3]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 65, 5),
            e("Must return a type that is assignable to int", 68, 19),
            e("No suitable method found that matches [invalidReturnType4]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 73, 5),
            e("Must return a type that is assignable to int", 76, 17),
            e("No suitable method found that matches [bad.InvalidConversionsMethods.Inner#privateMethod]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 80, 5),
            e("Must be public", 87, 28),
            e("No suitable method found that matches [bad.InvalidConversionsMethods.Inner#packagePrivateMethod]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 83, 5),
            e("Must be public", 91, 20)
        );
    }

    private void runBadTest(String className, ErrorCheck... expectations) {
        JavaFileObject file = forResource(String.format("bad/%s.java", className));

        CompileTester.UnsuccessfulCompilationClause clause = assertAbout(javaSource())
            .that(file)
            .processedWith(new ConfigurationProcessor())
            .failsToCompile();

        for (ErrorCheck expectation : expectations) {
            clause = clause
                .withErrorContaining(expectation.error)
                .in(file)
                .onLine(expectation.line)
                .atColumn(expectation.column)
                .and();
        }

        clause.withErrorCount(expectations.length);
    }

    private JavaFileObject loadExpectedFile(String resourceName) {
        try {
            List<String> sourceLines = Resources.readLines(Resources.getResource(resourceName), UTF_8);
            if (!isJavaxAnnotationProcessingGeneratedAvailable()) {
                replaceGeneratedImport(sourceLines);
            }
            String binaryName = resourceName
                .replace('/', '.')
                .replace(".java", "");
            return forSourceLines(binaryName, sourceLines);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isJavaxAnnotationProcessingGeneratedAvailable() {
        return SourceVersion.latestSupported().compareTo(SourceVersion.RELEASE_8) > 0;
    }

    private static void replaceGeneratedImport(List<String> sourceLines) {
        int i = 0;
        int firstImport = Integer.MAX_VALUE;
        int lastImport = -1;
        for (String line : sourceLines) {
            if (line.startsWith("import ") && !line.startsWith("import static ")) {
                firstImport = Math.min(firstImport, i);
                lastImport = Math.max(lastImport, i);
            }
            i++;
        }
        if (lastImport >= 0) {
            List<String> importLines = sourceLines.subList(firstImport, lastImport + 1);
            importLines.replaceAll(line ->
                line.startsWith("import javax.annotation.processing.Generated;")
                    ? "import javax.annotation.Generated;"
                    : line);
            importLines.sort(String.CASE_INSENSITIVE_ORDER);
        }
    }

    private static ErrorCheck e(String error, int line, int column) {
        return new ErrorCheck(error, line, column);
    }

    private static final class ErrorCheck {
        private final String error;
        private final int line;
        private final int column;

        private ErrorCheck(String error, int line, int column) {
            this.error = error;
            this.line = line;
            this.column = column;
        }
    }
}
