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
        "NullableParameters",
        "Conversions",
        "ConvertingParameters"
    })
    void positiveTest(String className) {
        assertAbout(javaSource())
            .that(forResource(String.format("positive/%s.java", className)))
            .processedWith(new ConfigurationProcessor())
            .compilesWithoutError()
            .and()
            .generatesSources(loadExpectedFile(String.format("expected/%s.java", className)));
    }

    @Test
    void baseClassMustBeAnInterface() {
        runNegativeTest(
            "BaseClassIsNotAnInterface",
            e(
                "The annotated configuration must be an interface.",
                25,
                17
            )
        );
    }

    @Test
    void failOnUnsupportedMethods() {
        runNegativeTest(
            "InvalidMethods",
            e("Unsupported return type: char", 29, 10),
            e("Unsupported return type: void", 31, 10),
            e("Unsupported return type: int[]", 33, 11),
            e("Method may not have any parameters", 35, 12),
            e("Method may not have any type parameters", 37, 11),
            e("Method may not have any type parameters", 39, 24),
            e("Method may not declare any exceptions to be thrown", 41, 9),
            e("Method may not declare any exceptions to be thrown", 43, 12)
        );
    }

    @Test
    void emptyKeyIsNotAllowed() {
        runNegativeTest(
            "EmptyKey",
            e("The key must not be empty", 28, 9),
            e("The key must not be empty", 31, 9)
        );
    }

    @Test
    void invalidAnnotationCombinations() {
        runNegativeTest(
            "InvalidAnnotationCombinations",
            e("The `@Parameter` annotation cannot be used together with the `@Key` annotation", 31, 9)
        );
    }

    @Test
    void invalidConversionsClassTargets() {
        runNegativeTest(
            "InvalidConversionsClasses",
            e("Empty conversion method is not allowed", 27, 5),
            e("Multiple possible candidates found: [multipleOverloads(java.lang.String), multipleOverloads(long)]", 30, 5),
            e("Method is ambiguous and a possible candidate for [multipleOverloads]", 33, 16),
            e("Method is ambiguous and a possible candidate for [multipleOverloads]", 37, 16),
            e("[negative.class.does.not.exist#foo] is not a valid fully qualified method name: The class [negative.class.does.not.exist] cannot be found", 41, 5),
            e("No suitable method found that matches [methodDoesNotExist]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 44, 5),
            e("No suitable method found that matches [negative.InvalidConversionsClasses#methodDoesNotExist]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 47, 5),
            e("[negative.InvalidConversionsClasses#] is not a valid fully qualified method name: it must start with a fully qualified class name followed by a '#' and then the method name", 50, 5)
        );
    }

    @Test
    void invalidConversionsMethodTargets() {
        runNegativeTest(
            "InvalidConversionsMethods",
            e("No suitable method found that matches [nonStatic]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 27, 5),
            e("Must be static", 31, 17),
            e("No suitable method found that matches [generic]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 36, 5),
            e("May not be generic", 39, 18),
            e("No suitable method found that matches [declaresThrows]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 44, 5),
            e("May not declare any exceptions", 47, 16),
            e("No suitable method found that matches [noParameters]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 52, 5),
            e("May only accept one parameter", 55, 16),
            e("No suitable method found that matches [multipleParameters]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 60, 5),
            e("May only accept one parameter", 63, 16),
            e("No suitable method found that matches [invalidReturnType1]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 68, 5),
            e("Must return a type that is assignable to int", 71, 19),
            e("No suitable method found that matches [invalidReturnType2]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 76, 5),
            e("Must return a type that is assignable to int", 79, 17),
            e("No suitable method found that matches [invalidReturnType3]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 84, 5),
            e("Must return a type that is assignable to int", 87, 19),
            e("No suitable method found that matches [invalidReturnType4]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 92, 5),
            e("Must return a type that is assignable to int", 95, 17),
            e("No suitable method found that matches [negative.InvalidConversionsMethods.Inner#privateMethod]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 99, 5),
            e("Must be public", 106, 28),
            e("No suitable method found that matches [negative.InvalidConversionsMethods.Inner#packagePrivateMethod]. Make sure that the method is static, public, unary, not generic, does not declare any exception and returns [int]", 102, 5),
            e("Must be public", 110, 20)
        );
    }

    private void runNegativeTest(String className, ErrorCheck... expectations) {
        JavaFileObject file = forResource(String.format("negative/%s.java", className));

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
            replaceGeneratedImport(sourceLines, isCompilingOnJdk8());
            String binaryName = resourceName
                .replace('/', '.')
                .replace(".java", "");
            return forSourceLines(binaryName, sourceLines);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isCompilingOnJdk8() {
        return SourceVersion.latestSupported().compareTo(SourceVersion.RELEASE_8) == 0;
    }

    private static void replaceGeneratedImport(List<String> sourceLines, boolean compilesOnJdk8) {
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
            if (compilesOnJdk8) {
                importLines.replaceAll(line ->
                    line.startsWith("import javax.annotation.processing.Generated;")
                        ? "import javax.annotation.Generated;"
                        : line);
            }
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
