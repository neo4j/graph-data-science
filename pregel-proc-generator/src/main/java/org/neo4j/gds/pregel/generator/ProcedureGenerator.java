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
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.pregel.proc.PregelBaseProc;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import javax.lang.model.element.Modifier;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ProcedureGenerator {

    private final TypeNames typeNames;
    private final String procedureBaseName;
    private final Optional<String> description;

    public ProcedureGenerator(
        TypeNames typeNames,
        String procedureBaseName,
        Optional<String> description
    ) {
        this.typeNames = typeNames;
        this.procedureBaseName = procedureBaseName;
        this.description = description;
    }

    public TypeSpec generate(GDSMode gdsMode, boolean requiresInverseIndex, Optional<AnnotationSpec> generatedAnnotationSpec) {
        var typeSpecBuilder = typeSpec(gdsMode, generatedAnnotationSpec).toBuilder();
        typeSpecBuilder.addMethod(procMethod(gdsMode));
        typeSpecBuilder.addMethod(procEstimateMethod(gdsMode));
        typeSpecBuilder.addMethod(procResultMethod(gdsMode));
        typeSpecBuilder.addMethod(newConfigMethod());
        typeSpecBuilder.addMethod(algorithmFactoryMethod());

        if (requiresInverseIndex) {
            typeSpecBuilder.addMethod(inverseIndexValidationOverride());
        }

        return typeSpecBuilder.build();
    }

    TypeSpec typeSpec(GDSMode gdsMode, Optional<AnnotationSpec> generatedAnnotationSpec) {
        var fullProcedureName = formatWithLocale("%s.%s", procedureBaseName, gdsMode.lowerCase());
        var gdsCallableAnnotationBuilder = AnnotationSpec
            .builder(GdsCallable.class)
            .addMember("name", "$S", fullProcedureName)
            .addMember("executionMode", "$T.$L", ExecutionMode.class, executionMode(gdsMode));
        description.ifPresent(description -> gdsCallableAnnotationBuilder.addMember("description", "$S", description));

        var typeSpecBuilder = TypeSpec
            .classBuilder(typeNames.procedure(gdsMode))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                typeNames.procedureBase(gdsMode),
                typeNames.algorithm(),
                typeNames.config()
            ))
            .addAnnotation(gdsCallableAnnotationBuilder.build());

        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);
        return typeSpecBuilder.build();
    }

    MethodSpec procMethod(GDSMode gdsMode) {
        var fullProcedureName = formatWithLocale("%s.%s", procedureBaseName, gdsMode.lowerCase());
        var methodBuilder = MethodSpec.methodBuilder(gdsMode.lowerCase())
            .addAnnotation(AnnotationSpec.builder(Procedure.class)
                .addMember("name", "$S", fullProcedureName)
                .addMember("mode", "$T.$L", Mode.class, neo4jProcedureMode(gdsMode))
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
        description.ifPresent(description -> methodBuilder.addAnnotation(
            AnnotationSpec.builder(Description.class)
                .addMember("value", "$S", description)
                .build()
        ));
        methodBuilder
            .addStatement("return $L(compute(graphName, configuration))", gdsMode.lowerCase())
            .returns(ParameterizedTypeName.get(
                ClassName.get(Stream.class),
                typeNames.procedureResult(gdsMode))
            );
        return methodBuilder.build();
    }

    MethodSpec procEstimateMethod(GDSMode gdsMode) {
        return estimateMethodSignature(gdsMode)
            .addAnnotation(AnnotationSpec.builder(Description.class)
                .addMember("value", "$T.ESTIMATE_DESCRIPTION", BaseProc.class)
                .build()
            )
            .addStatement("return computeEstimate(graphNameOrConfiguration, algoConfiguration)", gdsMode.lowerCase())
            .returns(ParameterizedTypeName.get(Stream.class, MemoryEstimateResult.class))
            .build();
    }

    private MethodSpec.Builder estimateMethodSignature(GDSMode gdsMode) {
        var fullProcedureName = formatWithLocale("%s.%s.estimate", procedureBaseName, gdsMode.lowerCase());
        return MethodSpec.methodBuilder("estimate")
            .addAnnotation(AnnotationSpec.builder(Procedure.class)
                .addMember("name", "$S", fullProcedureName)
                .addMember("mode", "$T.$L", Mode.class, Mode.READ)
                .build()
            )
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(Object.class, "graphNameOrConfiguration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "graphNameOrConfiguration").build()
                ).build()
            ).addParameter(ParameterSpec
                .builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "algoConfiguration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "algoConfiguration").build()
                ).build());
    }

    MethodSpec procResultMethod(GDSMode gdsMode) {
        switch (gdsMode) {
            case MUTATE:
            case STATS:
            case WRITE:
                return nonThrowingResultBuilderMethod(gdsMode);
            case STREAM:
                return throwingStreamResultMethod();
            default: throw new IllegalStateException("Unexpected value: " + gdsMode);
        }
    }

    MethodSpec newConfigMethod() {
        return MethodSpec.methodBuilder("newConfig")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(typeNames.config())
            .addParameter(String.class, "username")
            .addParameter(CypherMapWrapper.class, "config")
            .addStatement("return $T.of(config)", typeNames.config())
            .build();
    }

    MethodSpec algorithmFactoryMethod() {
        return MethodSpec.methodBuilder("algorithmFactory")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(
                ClassName.get(GraphAlgorithmFactory.class),
                typeNames.algorithm(),
                typeNames.config()
            ))
            .addParameter(ClassName.get(ExecutionContext.class), "executionContext")
            .addStatement("return new $T()", typeNames.algorithmFactory())
            .build();
    }

    MethodSpec inverseIndexValidationOverride() {
        return MethodSpec.methodBuilder("validationConfig")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(
                ClassName.get(ValidationConfiguration.class),
                typeNames.config()
            ))
            .addParameter(ClassName.get(ExecutionContext.class), "executionContext")
            .addStatement("return $T.ensureIndexValidation(executionContext.log(), executionContext.taskRegistryFactory())", PregelBaseProc.class)
            .build();
    }

    private MethodSpec throwingStreamResultMethod() {
        return MethodSpec.methodBuilder("streamResult")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(typeNames.procedureResult(GDSMode.STREAM))
            .addParameter(long.class, "originalNodeId")
            .addParameter(long.class, "internalNodeId")
            .addParameter(NodePropertyValues.class, "nodePropertyValues")
            .addStatement("throw new $T()", UnsupportedOperationException.class)
            .build();
    }

    private MethodSpec nonThrowingResultBuilderMethod(GDSMode gdsMode) {
        return MethodSpec.methodBuilder("resultBuilder")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(ParameterizedTypeName.get(
                    ClassName.get(AbstractResultBuilder.class),
                    typeNames.procedureResult(gdsMode)
                )
            ).addParameter(ParameterizedTypeName.get(
                ClassName.get(ComputationResult.class),
                typeNames.algorithm(),
                ClassName.get(PregelResult.class),
                typeNames.config()
            ), "computeResult")
            .addParameter(ExecutionContext.class, "executionContext")
            .addStatement("var ranIterations = computeResult.result().map(PregelResult::ranIterations).orElse(0)")
            .addStatement("var didConverge = computeResult.result().map(PregelResult::didConverge).orElse(false)")
            .addStatement("return new $T().withRanIterations(ranIterations).didConverge(didConverge)", typeNames.procedureResult(gdsMode).nestedClass("Builder"))
            .build();
    }

    private ExecutionMode executionMode(GDSMode mode) {
        switch (mode) {
            case STREAM: return ExecutionMode.STREAM;
            case WRITE: return ExecutionMode.WRITE_NODE_PROPERTY;
            case MUTATE: return ExecutionMode.MUTATE_NODE_PROPERTY;
            case STATS: return ExecutionMode.STATS;
            default: throw new IllegalArgumentException("Unsupported procedure mode: " + mode);
        }
    }

    private Mode neo4jProcedureMode(GDSMode mode) {
        switch (mode) {
            case STREAM:
            case MUTATE:
            case STATS:
                return Mode.READ;
            case WRITE:
                return Mode.WRITE;
            default: throw new IllegalArgumentException("Unsupported procedure mode: " + mode);
        }
    }
}
