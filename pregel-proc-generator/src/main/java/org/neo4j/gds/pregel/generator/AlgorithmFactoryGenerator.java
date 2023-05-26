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
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import javax.lang.model.element.Modifier;
import java.util.Optional;

public class AlgorithmFactoryGenerator {

    private final TypeNames typeNames;

    public AlgorithmFactoryGenerator(TypeNames typeNames) {
        this.typeNames = typeNames;
    }

    public TypeSpec generate(Optional<AnnotationSpec> generatedAnnotationSpec) {
        return typeSpec(generatedAnnotationSpec).toBuilder()
            .addMethod(buildMethod())
            .addMethod(taskNameMethod())
            .addMethod(progressTaskMethod())
            .addMethod(memoryEstimationMethod())
            .build();
    }

    TypeSpec typeSpec(Optional<AnnotationSpec> generatedAnnotationSpec) {
        var algorithmClassName = typeNames.algorithm();

        var typeSpecBuilder = TypeSpec
            .classBuilder(typeNames.algorithmFactory())
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(GraphAlgorithmFactory.class),
                algorithmClassName,
                typeNames.config()
            ));

        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);

        return typeSpecBuilder.build();
    }

    MethodSpec buildMethod() {
        return MethodSpec.methodBuilder("build")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(Graph.class, "graph")
            .addParameter(typeNames.config(), "configuration")
            .addParameter(ProgressTracker.class, "progressTracker")
            .returns(typeNames.algorithm())
            .addStatement("return new $T(graph, configuration, progressTracker)", typeNames.algorithm())
            .build();
    }

    MethodSpec taskNameMethod() {
        return MethodSpec.methodBuilder("taskName")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $T.class.getSimpleName()", typeNames.algorithm())
            .build();
    }

    MethodSpec progressTaskMethod() {
        return MethodSpec.methodBuilder("progressTask")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(Graph.class, "graph")
            .addParameter(typeNames.config(), "configuration")
            .returns(Task.class)
            .addStatement("return Pregel.progressTask(graph, configuration)", typeNames.algorithm())
            .build();
    }

    MethodSpec memoryEstimationMethod() {
        return MethodSpec.methodBuilder("memoryEstimation")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(MemoryEstimation.class)
            .addParameter(typeNames.config(), "configuration")
            .addStatement("var computation = new $T()", typeNames.computation())
            .addStatement(
                "return $T.memoryEstimation(computation.schema(configuration), computation.reducer().isEmpty(), configuration.isAsynchronous())",
                Pregel.class
            )
            .build();
    }
}
