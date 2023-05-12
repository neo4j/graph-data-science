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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.pregel.proc.PregelMutateComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelMutateResult;
import org.neo4j.gds.pregel.proc.PregelStatsComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelStatsResult;
import org.neo4j.gds.pregel.proc.PregelStreamComputationResultConsumer;
import org.neo4j.gds.pregel.proc.PregelStreamResult;
import org.neo4j.gds.pregel.proc.PregelWriteResult;

import javax.lang.model.element.Modifier;
import java.lang.reflect.Type;

import static org.neo4j.gds.beta.pregel.PregelGenerator.ALGORITHM_FACTORY_SUFFIX;
import static org.neo4j.gds.beta.pregel.PregelGenerator.ALGORITHM_SPECIFICATION_SUFFIX;
import static org.neo4j.gds.beta.pregel.PregelGenerator.ALGORITHM_SUFFIX;

public class SpecificationGenerator {

    private final String packageName;
    private final String computationName;

    SpecificationGenerator(String packageName, String computationName) {
        this.packageName = packageName;
        this.computationName = computationName;
    }

    TypeSpec.Builder typeSpec(TypeName configTypeName, GDSMode mode) {
        var className = derivedClassName(ALGORITHM_SPECIFICATION_SUFFIX);
        var algorithmClassName = derivedClassName(ALGORITHM_SUFFIX);
        var algorithmFactoryClassName = derivedClassName(ALGORITHM_FACTORY_SUFFIX);

        var typeSpecBuilder = TypeSpec
            .classBuilder(className)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(AlgorithmSpec.class),
                algorithmClassName,
                ClassName.get(PregelResult.class),
                configTypeName,
                ClassName.get(resultTypeForMode(mode)),
                algorithmFactoryClassName
                // TODO: add originating element
            ));
        // TODO: add generated annotation
        return typeSpecBuilder;
    }

    MethodSpec nameMethod() {
        var algorithmClassName = derivedClassName(ALGORITHM_SUFFIX);
        return MethodSpec.methodBuilder("name")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $T.class.getSimpleName()", algorithmClassName)
            .build();
    }

    MethodSpec algorithmFactoryMethod() {
        var algorithmFactoryClassName = derivedClassName(ALGORITHM_FACTORY_SUFFIX);
        return MethodSpec.methodBuilder("algorithmFactory")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(algorithmFactoryClassName)
            .addParameter(ExecutionContext.class, "executionContext")
            .addStatement("return new $T()", algorithmFactoryClassName)
            .build();
    }

    MethodSpec newConfigFunctionMethod(TypeName configTypeName) {
        return MethodSpec.methodBuilder("newConfigFunction")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(NewConfigFunction.class),
                    configTypeName
                )
            ).addStatement("return (__, userInput) -> $T.of(userInput)", PregelProcedureConfig.class)
            .build();
    }

    MethodSpec computationResultConsumerMethod(TypeName configTypeName, GDSMode mode) {
        var algorithmClassName = derivedClassName(ALGORITHM_SUFFIX);
        return MethodSpec.methodBuilder("computationResultConsumer")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(ComputationResultConsumer.class),
                    algorithmClassName,
                    configTypeName
                ))
            .addStatement("return new $T<>()", ClassName.get(computationResultConsumerTypeForMode(mode))
            ).build();
    }

    private ClassName derivedClassName(String suffix) {
        return ClassName.get(packageName, computationName + suffix);
    }

    private Type resultTypeForMode(GDSMode mode) {
        switch (mode) {
            case STATS: return PregelStatsResult.class;
            case WRITE: return PregelWriteResult.class;
            case MUTATE: return PregelMutateResult.class;
            case STREAM: return PregelStreamResult.class;
            default: throw new IllegalStateException("Unexpected value: " + mode);
        }
    }

    private Type computationResultConsumerTypeForMode(GDSMode mode) {
        switch (mode) {
            case STATS: return PregelStatsComputationResultConsumer.class;
//            case WRITE: return PregelWriteComputationResultConsumer.class;
            case MUTATE: return PregelMutateComputationResultConsumer.class;
            case STREAM: return PregelStreamComputationResultConsumer.class;
            default: throw new IllegalStateException("Unexpected value: " + mode);
        }
    }
}
