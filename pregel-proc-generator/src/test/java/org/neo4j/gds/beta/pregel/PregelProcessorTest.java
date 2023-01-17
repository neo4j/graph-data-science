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
package org.neo4j.gds.beta.pregel;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.ResourceUtil;

import javax.tools.JavaFileObject;
import java.util.Locale;

import static com.google.common.truth.Truth.assertAbout;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaFileObjects.forSourceLines;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@EnableRuleMigrationSupport
class PregelProcessorTest {

    @Rule
    final CompilationRule compilationRule = new CompilationRule();

    @ParameterizedTest
    @ValueSource(strings = {
        "Computation"
    })
    void positiveTest(String className) {
        assertAbout(javaSource())
            .that(forResource(String.format("positive/%s.java", className)))
            .processedWith(new PregelProcessor())
            .compilesWithoutError()
            .and()
            .generatesSources(
                loadExpectedFile(formatWithLocale("expected/%sStreamProc.java", className)),
                loadExpectedFile(formatWithLocale("expected/%sWriteProc.java", className)),
                loadExpectedFile(formatWithLocale("expected/%sMutateProc.java", className)),
                loadExpectedFile(formatWithLocale("expected/%sStatsProc.java", className)),
                loadExpectedFile(formatWithLocale("expected/%sAlgorithm.java", className))
            );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "BidirectionalComputation"
    })
    void positiveBiTest(String className) {
        assertAbout(javaSource())
            .that(forResource(String.format("positive/%s.java", className)))
            .processedWith(new PregelProcessor())
            .compilesWithoutError()
            .and()
            .generatesSources(
                loadExpectedFile(formatWithLocale("expected/%sStreamProc.java", className)),
                loadExpectedFile(formatWithLocale("expected/%sAlgorithm.java", className))
            );
    }

    @Test
    void baseClassMustBeAClass() {
        runNegativeTest(
            "BaseClassIsNotAClass",
            e(
                "The annotated Pregel computation must be a class.",
                34,
                8
            )
        );
    }

    @Test
    void baseClassMustHaveEmptyConstructor() {
        runNegativeTest(
            "BaseClassHasNoEmptyConstructor",
            e(
                "The annotated Pregel computation must have an empty constructor.",
                33,
                8
            )
        );
    }

    @Test
    void baseClassMustImplementPregelComputation() {
        runNegativeTest(
            "BaseClassIsNotAPregelComputation",
            e(
                "The annotated Pregel computation must implement the PregelComputation interface.",
                28,
                8
            )
        );
    }

    @Test
    void baseClassHasNoPregelProcedureConfig() {
        runNegativeTest(
            "BaseClassHasNoPregelProcedureConfig",
            e(
                "The annotated Pregel computation must have a configuration type which is a subtype of PregelProcedureConfiguration.",
                33,
                8
            )
        );
    }

    @Test
    void configurationMustHaveStaticFactoryMethod() {
        runNegativeTest(
            "ConfigurationHasNoFactoryMethod",
            e(
                "Missing method " +
                "'static ConfigurationHasNoFactoryMethod.ComputationConfig " +
                "of" +
                "(" +
                "org.neo4j.gds.core.CypherMapWrapper userConfig" +
                ")' " +
                "in ConfigurationHasNoFactoryMethod.ComputationConfig.",
                33,
                8
            )
        );
    }

    private void runNegativeTest(String className, ErrorCheck... expectations) {
        JavaFileObject file = forResource(String.format(Locale.ENGLISH, "negative/%s.java", className));

        CompileTester.UnsuccessfulCompilationClause clause = assertAbout(javaSource())
            .that(file)
            .processedWith(new PregelProcessor())
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
        var sourceLines = ResourceUtil.lines(resourceName);

        String binaryName = resourceName
            .replace('/', '.')
            .replace(".java", "");
        return forSourceLines(binaryName, sourceLines);
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
