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

    @Test
    void emptyClass() {
        runGoodTest("EmptyClass");
    }

    @Test
    void fieldTypes() {
        runGoodTest("FieldTypes");
    }

    @Test
    void defaultValues() {
        runGoodTest("DefaultValues");
    }

    @Test
    void inheritance() {
        runGoodTest("Inheritance");
    }

    @Test
    void ignoring() {
        runGoodTest("Ignores");
    }

    @Test
    void resolveNamingConflicts() {
        runGoodTest("NamingConflict");
    }

    @Test
    void keyRenames() {
        runGoodTest("KeyRenames");
    }

    @Test
    void parameters() {
        runGoodTest("Parameters");
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
            e("Unsupported return type: char", 9, 10),
            e("Unsupported return type: void", 11, 10),
            e("Unsupported return type: int[]", 13, 11),
            e("Method may not have any parameters", 15, 12),
            e("Method may not have any type parameters", 17, 11),
            e("Method may not have any type parameters", 19, 24),
            e("Method may not declare any exceptions to be thrown", 21, 9),
            e("Method may not declare any exceptions to be thrown", 23, 12)
        );
    }

    private void runGoodTest(String className) {
        assertAbout(javaSource())
            .that(forResource(String.format("good/%s.java", className)))
            .processedWith(new ConfigurationProcessor())
            .compilesWithoutError()
            .and()
            .generatesSources(loadExpectedFile(String.format("expected/%s.java", className)));
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
