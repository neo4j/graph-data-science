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
package org.neo4j.gds.junit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.engine.execution.BeforeEachMethodAdapter;
import org.junit.jupiter.engine.extension.ExtensionRegistry;

/**
 * A resolver that maps parameters from @ParameterizedTest to
 * parameters of methods annotated with @BeforeEach or @AfterEach.
 * <p>
 * Example:
 *
 * <pre>
 * {@code @ExtendWith(MappedParameterResolver.class)}
 * class MyTest {
 *   {@code @BeforeEach}
 *   void setup(int param) {
 *     // do something with param
 *   }
 *
 *   {@code @AfterEach}
 *   void teardown(int param) {
 *     // do something with param
 *   }
 *
 *   {@code @ParameterizedTest}
 *   {@code @ValueSource(ints = {1, 2, 3})}
 *   void test(int param) {
 *     // do something with param
 *   }
 * }
 * </pre>
 */
public class MappedParameterResolver implements BeforeEachMethodAdapter, ParameterResolver {

    // The default parameter resolver for @ParameterizedTest
    private ParameterResolver parameterizedTestParameterResolver;

    @Override
    public boolean supportsParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        if (isExecutedOnBeforeOrAfterEach(parameterContext)) {
            var mappedParameterContext = mappedParameterContext(parameterContext, extensionContext);
            return parameterizedTestParameterResolver.supportsParameter(mappedParameterContext, extensionContext);
        }
        return false;
    }

    @Override
    public Object resolveParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterizedTestParameterResolver.resolveParameter(
            mappedParameterContext(parameterContext, extensionContext),
            extensionContext
        );
    }

    @Override
    public void invokeBeforeEachMethod(ExtensionContext context, ExtensionRegistry registry) throws Throwable {
        this.parameterizedTestParameterResolver = registry.getExtensions(ParameterResolver.class)
            .stream()
            .filter(parameterResolver -> parameterResolver.getClass()
                .getName()
                .contains("ParameterizedTestParameterResolver"))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "ParameterizedTestParameterResolver not found. Test needs to be @ParameterizedTest")
            );
    }

    private static boolean isExecutedOnBeforeOrAfterEach(ParameterContext parameterContext) {
        return parameterContext.getDeclaringExecutable().isAnnotationPresent(BeforeEach.class) ||
            parameterContext.getDeclaringExecutable().isAnnotationPresent(AfterEach.class);
    }

    private static MappedParameterContext mappedParameterContext(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) {
        return new MappedParameterContext(
            parameterContext.getIndex(),
            extensionContext.getRequiredTestMethod().getParameters()[parameterContext.getIndex()],
            parameterContext.getTarget()
        );
    }
}
