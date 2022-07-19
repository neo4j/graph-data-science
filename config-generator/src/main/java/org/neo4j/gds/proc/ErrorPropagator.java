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
package org.neo4j.gds.proc;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.NameAllocator;

import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public final class ErrorPropagator {

    private ErrorPropagator() {}

    static void catchAndPropagateIllegalArgumentError(
        MethodSpec.Builder builder,
        String errorVarName,
        UnaryOperator<MethodSpec.Builder> statementFunc
    ) {
        builder.beginControlFlow("try");
        statementFunc.apply(builder);
        builder
            .nextControlFlow("catch ($T e)", IllegalArgumentException.class)
            .addStatement("$N.add(e)", errorVarName)
            .endControlFlow();
    }

    static void combineCollectedErrors(
        NameAllocator names,
        MethodSpec.Builder methodBuilder,
        String errorsVarName
    ) {
        String combinedErrorMsgVarName = names.newName("combinedErrorMsg");
        String combinedErrorVarName = names.newName("combinedError");
        methodBuilder.beginControlFlow("if(!$N.isEmpty())", errorsVarName)
            .beginControlFlow("if($N.size() == $L)", errorsVarName, 1)
            .addStatement("throw $N.get($L)", errorsVarName, 0)
            .nextControlFlow("else")
            .addStatement(
                "$1T $2N = $3N.stream().map($4T::getMessage)" +
                ".collect($5T.joining(System.lineSeparator() + $6S, $7S + System.lineSeparator() + $6S, $8S))",
                String.class,
                combinedErrorMsgVarName,
                errorsVarName,
                IllegalArgumentException.class,
                Collectors.class,
                "\t\t\t\t",
                "Multiple errors in configuration arguments:", //prefix
                "" // suffix
            )
            .addStatement(
                "$1T $2N = new $1T($3N)",
                IllegalArgumentException.class,
                combinedErrorVarName,
                combinedErrorMsgVarName
            )
            .addStatement(
                "$1N.forEach($2N -> $3N.addSuppressed($2N))",
                errorsVarName,
                names.newName("error"),
                combinedErrorVarName
            )
            .addStatement("throw $N", combinedErrorVarName)
            .endControlFlow()
            .endControlFlow();
    }
}
