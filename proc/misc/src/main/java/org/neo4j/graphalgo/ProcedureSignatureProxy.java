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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.utils.ExceptionUtil;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.procedure.Mode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class ProcedureSignatureProxy {

    static ProcedureSignature signature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        List<FieldSignature> outputSignature,
        Mode mode,
        boolean admin,
        String deprecated,
        String[] allowed,
        String description,
        String warning,
        boolean eager,
        boolean caseInsensitive,
        boolean systemProcedure,
        boolean internal,
        boolean allowExpiredCredentials
    ) {
        try {
            return (ProcedureSignature) CONSTRUCTOR.invoke(
                name,
                inputSignature,
                outputSignature,
                mode,
                admin,
                deprecated,
                allowed,
                description,
                warning,
                eager,
                caseInsensitive,
                systemProcedure,
                internal,
                allowExpiredCredentials
            );
        } catch (Throwable throwable) {
            ExceptionUtil.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    // The full method signature as expected from the final MethodHandle
    private static final Class<?>[] FULL_METHOD_SIGNATURE = {
        QualifiedName.class,
        List.class,
        List.class,
        Mode.class,
        boolean.class,
        String.class,
        String[].class,
        String.class,
        String.class,
        boolean.class,
        boolean.class,
        boolean.class,
        boolean.class,
        boolean.class,
    };

    // The first parameter is passed as positional arg to the methodType method.
    private static final Class<?>[] PRE_43_SIGNATURE_END = Arrays.copyOfRange(
        FULL_METHOD_SIGNATURE,
        1,
        ProcedureSignatureProxy.FULL_METHOD_SIGNATURE.length - 1
    );

    // 4.3 introduces a new parameter at the end: boolean allowExpiredCredentials
    private static final Class<?>[] POST_43_SIGNATURE_END = Arrays.copyOfRange(
        FULL_METHOD_SIGNATURE,
        1,
        FULL_METHOD_SIGNATURE.length
    );

    private static final MethodHandle CONSTRUCTOR = signatureConstructor();

    /**
     * Neo4j version compatible way of finding the constructor for a ProcedureSignature
     */
    private static MethodHandle signatureConstructor() {
        try {
            var constructor = findKnownSignatureConstructor();
            verifyConstructorHandle(constructor);
            return constructor;
        } catch (Exception e) {
            // No dice, there is a new constructor and we need to write code to support it.

            // Instead of throwing here and failing the database in full
            // we create a handle that always throws the exception when it is invoked
            // If no one uses this feature, the error is never seen.
            // Late-bind all the things!
            var error = new IllegalStateException(
                "Could find a way to register a dynamic procedure on the fly."
            );
            error.addSuppressed(e);

            var throwError = MethodHandles.throwException(ProcedureSignature.class, IllegalStateException.class);
            var throwingHandle = throwError.bindTo(error);
            // drop all args of the full signature and throw instead
            return MethodHandles.dropArguments(throwingHandle, 0, FULL_METHOD_SIGNATURE);
        }
    }

    private static MethodHandle findKnownSignatureConstructor() throws NoSuchMethodException, IllegalAccessException {
        var lookup = MethodHandles.lookup();
        try {
            // Try to find a matching constructor first
            return lookup.findConstructor(
                ProcedureSignature.class,
                MethodType.methodType(void.class, QualifiedName.class, POST_43_SIGNATURE_END)
            );
        } catch (NoSuchMethodException before43orUnknownFutureVersion) {
            // We're probably on a version before 4.3 - or there is a new compat issue
            // Tru to check for the previous constructor first
            var constructor42 = lookup.findConstructor(
                ProcedureSignature.class,
                MethodType.methodType(void.class, QualifiedName.class, PRE_43_SIGNATURE_END)
            );
            // accept another boolean parameter and drop it before calling
            // the actual constructor
            return MethodHandles.dropArguments(
                constructor42,
                // we drop the last param of the new signature
                FULL_METHOD_SIGNATURE.length - 1,
                boolean.class
            );
        }
    }

    private static void verifyConstructorHandle(MethodHandle handle) {
        var methodType = handle.type();
        if (methodType.returnType() != ProcedureSignature.class) {
            throw new IllegalArgumentException(formatWithLocale(
                "The constructor must return a [%s]. It returns a [%s] instead",
                ProcedureSignature.class.getSimpleName(),
                methodType.returnType()
            ));
        }
        for (int paramPos = 0; paramPos < FULL_METHOD_SIGNATURE.length; paramPos++) {
            Class<?> parameterType = FULL_METHOD_SIGNATURE[paramPos];
            if (paramPos >= methodType.parameterCount()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The constructor must accept a [%s] in parameter position %d. It does not have enough parameters to do so.",
                    parameterType,
                    // show errors in human 1-based index form
                    paramPos + 1
                ));
            }
            if (methodType.parameterType(paramPos) != parameterType) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The constructor must accept a [%s] in parameter position %d. It accepts a [%s] instead.",
                    parameterType,
                    // show errors in human 1-based index form
                    paramPos + 1,
                    methodType.parameterType(paramPos)
                ));
            }
        }
    }

    private ProcedureSignatureProxy() {}
}
