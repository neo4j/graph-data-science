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
package org.neo4j.gds.compat;

import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.ProcedureView;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.EnumMap;
import java.util.stream.Stream;

@CompatSince(minor = 25)
public final class QueryLanguageCompat {

    private QueryLanguageCompat() {}

    @FunctionalInterface
    public interface GetAllProcedures {
        Stream<ProcedureSignature> get(ProcedureView procedureView);
    }

    @FunctionalInterface
    public interface GetAllFunctions {
        Stream<UserFunctionSignature> get(ProcedureView procedureView);
    }

    @FunctionalInterface
    public interface GetAllAggregatingFunctions {
        Stream<UserFunctionSignature> get(ProcedureView procedureView);
    }

    public static final class ProcedureViewCompat {
        private final GetAllProcedures getAllProcedures;
        private final GetAllAggregatingFunctions getAllAggregatingFunctions;
        private final GetAllFunctions getAllNonAggregatingFunction;

        private ProcedureViewCompat(
            GetAllProcedures getAllProcedures,
            GetAllAggregatingFunctions getAllAggregatingFunctions,
            GetAllFunctions getAllNonAggregatingFunction
        ) {
            this.getAllProcedures = getAllProcedures;
            this.getAllAggregatingFunctions = getAllAggregatingFunctions;
            this.getAllNonAggregatingFunction = getAllNonAggregatingFunction;
        }

        public Stream<ProcedureSignature> getAllProcedures(ProcedureView procedureView) {
            return this.getAllProcedures.get(procedureView);
        }

        public Stream<UserFunctionSignature> getAllAggregatingFunctions(ProcedureView procedureView) {
            return this.getAllAggregatingFunctions.get(procedureView);
        }

        public Stream<UserFunctionSignature> getAllNonAggregatingFunctions(ProcedureView procedureView) {
            return this.getAllNonAggregatingFunction.get(procedureView);
        }
    }

    public enum LanguageScope {
        /**
         * The Cypher language as of Neo4j 2025.x
         */
        CYPHER_25(0),
        /**
         * The Cypher language as of Neo4j 5.x
         */
        CYPHER_5(1);

        private final int index;

        LanguageScope(int index) {
            this.index = index;
        }
    }


    private static final EnumMap<LanguageScope, ProcedureViewCompat> COMPAT_IMPLS = new EnumMap<>(LanguageScope.class);

    public static ProcedureViewCompat create() {
        return create(LanguageScope.CYPHER_5);
    }

    public static ProcedureViewCompat create(LanguageScope languageScope) {
        return COMPAT_IMPLS.computeIfAbsent(languageScope, QueryLanguageCompat::uncheckedCreate);
    }

    public static Object convertLanguageScope(MethodHandles.Lookup lookup, LanguageScope languageScope)
    throws ClassNotFoundException, IllegalAccessException {
        Class<?> languageScopeClass;
        try {
            languageScopeClass = lookup.findClass("org.neo4j.kernel.api.QueryLanguage");
        } catch (ClassNotFoundException e) {
            languageScopeClass = lookup.findClass("org.neo4j.kernel.api.CypherScope");
        }
        return languageScopeClass.getEnumConstants()[languageScope.index];
    }

    private static ProcedureViewCompat uncheckedCreate(LanguageScope languageScope) {
        try {
            return internalCreate(languageScope);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(
                "GDS is not compatible with this version of Neo4j." +
                    " Please upgrade to a version that is compatible with GDS" +
                    " or upgrade to a version of GDS that is compatible with your version of Neo4j.",
                e
            );
        }
    }

    private static ProcedureViewCompat internalCreate(LanguageScope languageScope) throws Throwable {
        var lookup = MethodHandles.lookup();
        var actualLanguageScope = convertLanguageScope(lookup, languageScope);

        var getAllProcedures = createInterface(
            lookup,
            GetAllProcedures.class,
            "getAllProcedures",
            actualLanguageScope
        );

        var getAllAggregatingFunctions = createInterface(
            lookup,
            GetAllAggregatingFunctions.class,
            "getAllAggregatingFunctions",
            actualLanguageScope
        );

        var getAllNonAggregatingFunctions = createInterface(
            lookup,
            GetAllFunctions.class,
            "getAllNonAggregatingFunctions",
            actualLanguageScope
        );

        return new ProcedureViewCompat(getAllProcedures, getAllAggregatingFunctions, getAllNonAggregatingFunctions);
    }

    private static <T> T createInterface(
        MethodHandles.Lookup lookup,
        Class<T> interfaceClass,
        String methodName,
        Object languageScope
    ) throws Throwable {
        var languageScopeClass = languageScope.getClass();

        var target = lookup.findVirtual(
            ProcedureView.class,
            methodName,
            MethodType.methodType(Stream.class, languageScopeClass)
        );
        target = MethodHandles.insertArguments(target, 1, languageScope);

        var methodType = target.type();
        var callSite = LambdaMetafactory.metafactory(
            lookup,
            // name of the method defined in the target functional interface
            "get",
            // type to be implemented and captured objects
            MethodType.methodType(interfaceClass, MethodHandle.class),
            // signature after type erasure
            methodType,
            // method handle to transform
            MethodHandles.exactInvoker(methodType),
            // actual interface method signature (same as erased because of non-generic interface)
            methodType
        );
        return interfaceClass.cast(callSite.getTarget().invoke(target));
    }
}
