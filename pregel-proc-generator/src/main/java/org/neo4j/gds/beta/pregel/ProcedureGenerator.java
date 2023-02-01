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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.pregel.proc.PregelBaseProc;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class ProcedureGenerator extends PregelGenerator {

    final PregelValidation.Spec pregelSpec;

    ProcedureGenerator(Elements elementUtils, SourceVersion sourceVersion, PregelValidation.Spec pregelSpec) {
        super(elementUtils, sourceVersion);
        this.pregelSpec = pregelSpec;
    }

    static TypeSpec forMode(
        GDSMode mode,
        Elements elementUtils,
        SourceVersion sourceVersion,
        PregelValidation.Spec pregelSpec
    ) {
        switch (mode) {
            case STREAM: return new StreamProcedureGenerator(elementUtils, sourceVersion, pregelSpec).typeSpec();
            case WRITE: return new WriteProcedureGenerator(elementUtils, sourceVersion, pregelSpec).typeSpec();
            case MUTATE: return new MutateProcedureGenerator(elementUtils, sourceVersion, pregelSpec).typeSpec();
            case STATS: return new StatsProcedureGenerator(elementUtils, sourceVersion, pregelSpec).typeSpec();
            default: throw new IllegalArgumentException("Unsupported procedure mode: " + mode);
        }
    }

    abstract GDSMode procGdsMode();

    abstract Mode procExecMode();

    abstract Class<?> procBaseClass();

    abstract Class<?> procResultClass();

    abstract MethodSpec procResultMethod();

    TypeSpec typeSpec() {
        var configTypeName = pregelSpec.configTypeName();
        var procedureClassName = computationClassName(pregelSpec, procGdsMode().camelCase() + PROCEDURE_SUFFIX);
        var algorithmClassName = computationClassName(pregelSpec, ALGORITHM_SUFFIX);

        var typeSpecBuilder = getTypeSpecBuilder(configTypeName, procedureClassName, algorithmClassName);

        addGeneratedAnnotation(typeSpecBuilder);

        typeSpecBuilder.addMethod(procMethod());
        typeSpecBuilder.addMethod(procEstimateMethod());
        typeSpecBuilder.addMethod(procResultMethod());
        typeSpecBuilder.addMethod(newConfigMethod());
        typeSpecBuilder.addMethod(algorithmFactoryMethod(algorithmClassName));

        if (pregelSpec.requiresInverseIndex()) {
            typeSpecBuilder.addMethod(validationConfigMethod());
        }

        return typeSpecBuilder.build();
    }

    @NotNull
    private TypeSpec.Builder getTypeSpecBuilder(
        com.squareup.javapoet.TypeName configTypeName,
        ClassName procedureClassName,
        ClassName algorithmClassName
    ) {

        ExecutionMode executionMode;
        switch (procGdsMode()) {
            case STATS: executionMode = ExecutionMode.STATS; break;
            case WRITE: executionMode = ExecutionMode.WRITE_NODE_PROPERTY; break;
            case MUTATE: executionMode = ExecutionMode.MUTATE_NODE_PROPERTY; break;
            case STREAM: executionMode = ExecutionMode.STREAM; break;
            default: throw new IllegalArgumentException("Unsupported procedure mode: " + procGdsMode());
        }

        var gdsCallableAnnotationBuilder = AnnotationSpec
            .builder(GdsCallable.class)
            .addMember("name", "$S", formatWithLocale("%s.%s", pregelSpec.procedureName(), procGdsMode().lowerCase()))
            .addMember("executionMode", "$T.$L", ExecutionMode.class, executionMode);
        pregelSpec.description().ifPresent(description ->
            gdsCallableAnnotationBuilder.addMember("description", "$S", description)
        );

        return TypeSpec
            .classBuilder(procedureClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(procBaseClass()),
                algorithmClassName,
                configTypeName
            ))
            .addAnnotation(gdsCallableAnnotationBuilder.build())
            .addOriginatingElement(pregelSpec.element());
    }

    private MethodSpec procMethod() {
        var methodBuilder = procMethodSignature(procExecMode());
        pregelSpec.description().ifPresent(description -> methodBuilder.addAnnotation(
            AnnotationSpec.builder(Description.class)
                .addMember("value", "$S", description)
                .build()
        ));
        return methodBuilder
            .addStatement("return $L(compute(graphName, configuration))", procGdsMode().lowerCase())
            .returns(ParameterizedTypeName.get(Stream.class, procResultClass()))
            .build();
    }

    private MethodSpec procEstimateMethod() {
        return estimateMethodSignature()
            .addAnnotation(AnnotationSpec.builder(Description.class)
                .addMember("value", "$T.ESTIMATE_DESCRIPTION", BaseProc.class)
                .build()
            )
            .addStatement("return computeEstimate(graphNameOrConfiguration, algoConfiguration)", procGdsMode().lowerCase())
            .returns(ParameterizedTypeName.get(Stream.class, MemoryEstimateResult.class))
            .build();
    }

    private MethodSpec.@NotNull Builder procMethodSignature(Mode procExecMode) {
        return MethodSpec.methodBuilder(procGdsMode().lowerCase())
            .addAnnotation(AnnotationSpec.builder(Procedure.class)
                .addMember(
                    "name",
                    "$S",
                    formatWithLocale("%s.%s", pregelSpec.procedureName(), procGdsMode().lowerCase())
                )
                .addMember("mode", "$T.$L", Mode.class, procExecMode)
                .build()
            )
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(String.class, "graphName")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "graphName")
                    .build())
                .build())
            .addParameter(ParameterSpec
                .builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "configuration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "configuration")
                    .addMember("defaultValue", "$S", "{}")
                    .build())
                .build());
    }

    @NotNull
    private MethodSpec.Builder estimateMethodSignature() {
        return MethodSpec.methodBuilder("estimate")
            .addAnnotation(AnnotationSpec.builder(Procedure.class)
                .addMember(
                    "name",
                    "$S",
                    formatWithLocale("%s.%s.estimate", pregelSpec.procedureName(), procGdsMode().lowerCase())
                )
                .addMember("mode", "$T.$L", Mode.class, Mode.READ)
                .build()
            )
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(Object.class, "graphNameOrConfiguration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "graphNameOrConfiguration")
                    .build())
                .build())
            .addParameter(ParameterSpec
                .builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "algoConfiguration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "algoConfiguration")
                    .build())
                .build());
    }

    private MethodSpec newConfigMethod() {
        return MethodSpec.methodBuilder("newConfig")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .addParameter(String.class, "username")
            .addParameter(CypherMapWrapper.class, "config")
            .returns(pregelSpec.configTypeName())
            .addStatement("return $T.of(config)", pregelSpec.configTypeName())
            .build();
    }

    private MethodSpec algorithmFactoryMethod(ClassName algorithmClassName) {
        TypeSpec anonymousFactoryType = TypeSpec.anonymousClassBuilder("")
            .superclass(ParameterizedTypeName.get(
                ClassName.get(GraphAlgorithmFactory.class),
                algorithmClassName,
                pregelSpec.configTypeName()
            ))
            .addMethod(MethodSpec.methodBuilder("build")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(Graph.class, "graph")
                .addParameter(pregelSpec.configTypeName(), "configuration")
                .addParameter(ProgressTracker.class, "progressTracker")
                .returns(algorithmClassName)
                .addStatement("return new $T(graph, configuration, progressTracker)", algorithmClassName)
                .build()
            )
            .addMethod(MethodSpec.methodBuilder("taskName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $T.class.getSimpleName()", algorithmClassName)
                .build()
            )
            .addMethod(MethodSpec.methodBuilder("progressTask")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(Graph.class, "graph")
                .addParameter(pregelSpec.configTypeName(), "configuration")
                .returns(Task.class)
                .addStatement("return Pregel.progressTask(graph, configuration)", algorithmClassName)
                .build())
            .addMethod(MethodSpec.methodBuilder("memoryEstimation")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(MemoryEstimation.class)
                .addParameter(pregelSpec.configTypeName(), "configuration")
                .addStatement("var computation = new $T()", computationClassName(pregelSpec, ""))
                .addStatement(
                    "return $T.memoryEstimation(computation.schema(configuration), computation.reducer().isEmpty(), configuration.isAsynchronous())",
                    Pregel.class
                )
                .build()
            )
            .build();

        return MethodSpec.methodBuilder("algorithmFactory")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(
                ClassName.get(GraphAlgorithmFactory.class),
                algorithmClassName,
                pregelSpec.configTypeName()
            ))
            .addStatement("return $L", anonymousFactoryType)
            .build();
    }

    private MethodSpec validationConfigMethod() {
        return MethodSpec.methodBuilder("validationConfig")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(
                ClassName.get(ValidationConfiguration.class),
                pregelSpec.configTypeName()
            ))
            .addStatement("return $T.ensureIndexValidation(log, executionContext().taskRegistryFactory())", PregelBaseProc.class)
            .build();
    }
}
