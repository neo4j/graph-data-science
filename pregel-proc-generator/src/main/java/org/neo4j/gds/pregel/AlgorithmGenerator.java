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
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.pregel.generator.TypeNames;

import javax.lang.model.element.Modifier;
import java.util.Map;
import java.util.Optional;

class AlgorithmGenerator extends PregelGenerator {
    private final TypeNames typeNames;

    AlgorithmGenerator(Optional<AnnotationSpec> generatedAnnotationSpec, TypeNames typeNames) {
        super(generatedAnnotationSpec);
        this.typeNames = typeNames;
    }

    TypeSpec typeSpec() {
        var typeSpecBuilder = TypeSpec
            .classBuilder(typeNames.algorithm())
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(Algorithm.class),
                ClassName.get(PregelResult.class)
            ));

        addGeneratedAnnotation(typeSpecBuilder);

        typeSpecBuilder.addField(pregelJobField());
        typeSpecBuilder.addMethod(constructor());
        typeSpecBuilder.addMethod(setTerminatonFlag());
        typeSpecBuilder.addMethod(computeMethod());

        return typeSpecBuilder.build();
    }

    private FieldSpec pregelJobField() {
        return FieldSpec
            .builder(
                ParameterizedTypeName.get(
                    ClassName.get(Pregel.class),
                    typeNames.config()
                ),
                "pregelJob",
                Modifier.PRIVATE,
                Modifier.FINAL
            )
            .build();
    }

    private MethodSpec constructor() {
        var configurationVar = "configuration";
        return MethodSpec.constructorBuilder()
            .addParameter(Graph.class, "graph")
            .addParameter(typeNames.config(), configurationVar)
            .addParameter(ProgressTracker.class, "progressTracker")
            .addStatement(CodeBlock.builder().add("super(progressTracker)").build())
            .addStatement(
                CodeBlock.builder().addNamed(
                    "this.pregelJob = $pregel:T.create(" +
                    "graph, " +
                    "$config:N, " +
                    "new $computation:T(), " +
                    "$pools:T.DEFAULT, " +
                    "progressTracker" +
                    ")",
                    Map.of(
                        "pregel", Pregel.class,
                        "pools", Pools.class,
                        "config", configurationVar,
                        "computation", typeNames.computation()
                    )
                )
                    .build()
            )
            .build();
    }

    private MethodSpec computeMethod() {
        return MethodSpec.methodBuilder("compute")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(PregelResult.class)
            .addStatement("return pregelJob.run()")
            .build();
    }

    private MethodSpec setTerminatonFlag() {
        return MethodSpec.methodBuilder("setTerminationFlag")
            .addAnnotation(Override.class)
            .addParameter(TerminationFlag.class, "terminationFlag")
            .addModifiers(Modifier.PUBLIC)
            .addStatement("super.setTerminationFlag(terminationFlag)")
            .addStatement("pregelJob.setTerminationFlag(terminationFlag)")
            .build();
    }
}
