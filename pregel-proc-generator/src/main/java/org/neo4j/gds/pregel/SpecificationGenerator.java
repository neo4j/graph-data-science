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
package org.neo4j.gds.pregel;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.pregel.generator.TypeNames;

import javax.lang.model.element.Modifier;
import java.util.Optional;
import java.util.stream.Stream;

public class SpecificationGenerator {

    private final TypeNames typeNames;

    SpecificationGenerator(TypeNames typeNames) {
        this.typeNames = typeNames;
    }

    TypeSpec generate(GDSMode mode, Optional<AnnotationSpec> generatedAnnotationSpec) {
        return typeSpec(mode, generatedAnnotationSpec).toBuilder()
            .addMethod(nameMethod())
            .addMethod(algorithmFactoryMethod())
            .addMethod(newConfigFunctionMethod())
            .addMethod(computationResultConsumerMethod(mode))
            .build();
    }

    TypeSpec typeSpec(GDSMode mode, Optional<AnnotationSpec> generatedAnnotationSpec) {
        var typeSpecBuilder = TypeSpec
            .classBuilder(typeNames.specification(mode))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addSuperinterface(ParameterizedTypeName.get(
                ClassName.get(AlgorithmSpec.class),
                typeNames.algorithm(),
                ClassName.get(PregelResult.class),
                typeNames.config(),
                ParameterizedTypeName.get(
                    ClassName.get(Stream.class),
                    typeNames.procedureResult(mode)
                ),
                typeNames.algorithmFactory()
            ));

        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);

        return typeSpecBuilder.build();
    }

    MethodSpec nameMethod() {
        return MethodSpec.methodBuilder("name")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $T.class.getSimpleName()", typeNames.algorithm())
            .build();
    }

    MethodSpec algorithmFactoryMethod() {
        return MethodSpec.methodBuilder("algorithmFactory")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(typeNames.algorithmFactory())
            .addParameter(ExecutionContext.class, "executionContext")
            .addStatement("return new $T()", typeNames.algorithmFactory())
            .build();
    }

    MethodSpec newConfigFunctionMethod() {
        return MethodSpec.methodBuilder("newConfigFunction")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(NewConfigFunction.class),
                    typeNames.config()
                )
            ).addStatement("return (__, userInput) -> $T.of(userInput)", typeNames.config())
            .build();
    }

    MethodSpec computationResultConsumerMethod(GDSMode mode) {
        return MethodSpec.methodBuilder("computationResultConsumer")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(ComputationResultConsumer.class),
                    typeNames.algorithm(),
                    ClassName.get(PregelResult.class),
                    typeNames.config(),
                    ParameterizedTypeName.get(
                        ClassName.get(Stream.class),
                        typeNames.procedureResult(mode)
                    )
                )
            )
            .addStatement("return new $T<>()", typeNames.computationResultConsumer(mode))
            .build();
    }
}
