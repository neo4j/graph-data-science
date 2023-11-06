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
package org.neo4j.gds.pregel.generator;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import javax.lang.model.element.Modifier;
import java.util.Optional;

public class AlgorithmGenerator {
    private final TypeNames typeNames;

    public AlgorithmGenerator(TypeNames typeNames) {
        this.typeNames = typeNames;
    }

    public TypeSpec generate(Optional<AnnotationSpec> generatedAnnotationSpec) {
        return typeSpec(generatedAnnotationSpec).toBuilder()
            .addField(pregelJobField())
            .addMethod(constructor())
            .addMethod(setTerminatonFlag())
            .addMethod(computeMethod())
            .build();
    }

    TypeSpec typeSpec(Optional<AnnotationSpec> generatedAnnotationSpec) {
        var typeSpecBuilder = TypeSpec
            .classBuilder(typeNames.algorithm())
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(Algorithm.class),
                ClassName.get(PregelResult.class)
            ));

        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);

        return typeSpecBuilder.build();
    }

    FieldSpec pregelJobField() {
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

    MethodSpec constructor() {
        return MethodSpec.constructorBuilder()
            .addParameter(Graph.class, "graph")
            .addParameter(typeNames.config(), "configuration")
            .addParameter(ProgressTracker.class, "progressTracker")
            .addStatement("super(progressTracker)")
            .addStatement("var computation = new $T()", typeNames.computation())
            .addStatement(
                "this.pregelJob = $T.create(graph, configuration, computation, $T.INSTANCE, progressTracker)",
                Pregel.class,
                DefaultPool.class
            ).build();
    }

    MethodSpec computeMethod() {
        return MethodSpec.methodBuilder("compute")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(PregelResult.class)
            .addStatement("return pregelJob.run()")
            .build();
    }

    MethodSpec setTerminatonFlag() {
        return MethodSpec.methodBuilder("setTerminationFlag")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TerminationFlag.class, "terminationFlag")
            .addStatement("super.setTerminationFlag(terminationFlag)")
            .addStatement("pregelJob.setTerminationFlag(terminationFlag)")
            .build();
    }
}
