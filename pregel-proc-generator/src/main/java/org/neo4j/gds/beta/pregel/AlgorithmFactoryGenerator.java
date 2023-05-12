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
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;

public class AlgorithmFactoryGenerator extends PregelGenerator {
    private final PregelValidation.Spec pregelSpec;

    AlgorithmFactoryGenerator(Elements elementUtils, SourceVersion sourceVersion, PregelValidation.Spec pregelSpec) {
        super(elementUtils, sourceVersion);
        this.pregelSpec = pregelSpec;
    }

    TypeSpec typeSpec() {
        var className = computationClassName(pregelSpec, ALGORITHM_FACTORY_SUFFIX);
        var algorithmClassName = computationClassName(pregelSpec, ALGORITHM_SUFFIX);

        var typeSpecBuilder = TypeSpec
            .classBuilder(className)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(GraphAlgorithmFactory.class),
                algorithmClassName,
                pregelSpec.configTypeName()
            ))
            .addOriginatingElement(pregelSpec.element());

        addGeneratedAnnotation(typeSpecBuilder);

        typeSpecBuilder.addMethod(buildMethod(algorithmClassName));
        typeSpecBuilder.addMethod(taskNameMethod(algorithmClassName));
        typeSpecBuilder.addMethod(progressTaskMethod(algorithmClassName));
        typeSpecBuilder.addMethod(memoryEstimationMethod());

        return typeSpecBuilder.build();
    }

    private MethodSpec buildMethod(ClassName algorithmClassName) {
        return MethodSpec.methodBuilder("build")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(Graph.class, "graph")
            .addParameter(pregelSpec.configTypeName(), "configuration")
            .addParameter(ProgressTracker.class, "progressTracker")
            .returns(algorithmClassName)
            .addStatement("return new $T(graph, configuration, progressTracker)", algorithmClassName)
            .build();
    }

    private MethodSpec taskNameMethod(ClassName algorithmClassName) {
        return MethodSpec.methodBuilder("taskName")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $T.class.getSimpleName()", algorithmClassName)
            .build();
    }

    private MethodSpec progressTaskMethod(ClassName algorithmClassName) {
        return MethodSpec.methodBuilder("progressTask")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(Graph.class, "graph")
            .addParameter(pregelSpec.configTypeName(), "configuration")
            .returns(Task.class)
            .addStatement("return Pregel.progressTask(graph, configuration)", algorithmClassName)
            .build();
    }

    private MethodSpec memoryEstimationMethod() {
        return MethodSpec.methodBuilder("memoryEstimation")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(MemoryEstimation.class)
            .addParameter(pregelSpec.configTypeName(), "configuration")
            .addStatement("var computation = new $T()", computationClassName(pregelSpec, ""))
            .addStatement(
                "return $T.memoryEstimation(computation.schema(configuration), computation.reducer().isEmpty(), configuration.isAsynchronous())",
                Pregel.class
            )
            .build();
    }
}
