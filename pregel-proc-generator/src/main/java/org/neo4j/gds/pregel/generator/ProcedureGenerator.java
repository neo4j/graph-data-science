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
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.pregel.proc.PregelCompanion;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
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

    public TypeSpec generate(GDSMode gdsMode, Optional<AnnotationSpec> generatedAnnotationSpec) {
        var typeSpecBuilder = typeSpec(gdsMode, generatedAnnotationSpec).toBuilder();

        if (gdsMode == GDSMode.WRITE) {
            typeSpecBuilder.addField(nodeExporterBuilderField());
            typeSpecBuilder.addMethod(executionContextOverride());
        }

        typeSpecBuilder.addMethod(procMethod(gdsMode));
        typeSpecBuilder.addMethod(procEstimateMethod(gdsMode));

        return typeSpecBuilder.build();
    }

    TypeSpec typeSpec(GDSMode gdsMode, Optional<AnnotationSpec> generatedAnnotationSpec) {
        var typeSpecBuilder = TypeSpec
            .classBuilder(typeNames.procedure(gdsMode))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(BaseProc.class);
        generatedAnnotationSpec.ifPresent(typeSpecBuilder::addAnnotation);
        return typeSpecBuilder.build();
    }

    FieldSpec nodeExporterBuilderField() {
        return FieldSpec.builder(NodePropertyExporterBuilder.class, "nodePropertyExporterBuilder")
            .addAnnotation(AnnotationSpec.builder(Context.class).build())
            .addModifiers(Modifier.PUBLIC)
            .build();

    }

    MethodSpec executionContextOverride() {
        return MethodSpec.methodBuilder("executionContext")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ExecutionContext.class)
            .addStatement("return super.executionContext().withNodePropertyExporterBuilder(nodePropertyExporterBuilder)")
            .build();
    }

    MethodSpec procMethod(GDSMode gdsMode) {
        var fullProcedureName = formatWithLocale("%s.%s", procedureBaseName, gdsMode.lowerCase());
        var methodBuilder = MethodSpec.methodBuilder(gdsMode.lowerCase())
            .addAnnotation(AnnotationSpec.builder(Procedure.class)
                .addMember("name", "$S", fullProcedureName)
                .addMember("mode", "$T.$L", Mode.class, neo4jProcedureMode(gdsMode))
                .build()
            );
        description.ifPresent(description -> methodBuilder.addAnnotation(
            AnnotationSpec.builder(Description.class)
                .addMember("value", "$S", description)
                .build()
        ));
        methodBuilder
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(
                ClassName.get(Stream.class),
                typeNames.procedureResult(gdsMode))
            )
            .addParameter(ParameterSpec.builder(String.class, "graphName")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "graphName")
                    .build())
                .build())
            .addParameter(ParameterSpec.builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "configuration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "configuration")
                    .addMember("defaultValue", "$S", "{}")
                    .build())
                .build())
            .addStatement("var specification = new $T()", typeNames.specification(gdsMode))
            .addStatement("var executor = new $T<>(specification, executionContext())", ProcedureExecutor.class)
            .addStatement("return executor.compute(graphName, configuration)");
        return methodBuilder.build();
    }

    MethodSpec procEstimateMethod(GDSMode gdsMode) {
        var fullProcedureName = formatWithLocale("%s.%s.estimate", procedureBaseName, gdsMode.lowerCase());
        return MethodSpec.methodBuilder("estimate")
            .addAnnotation(AnnotationSpec.builder(Procedure.class)
                .addMember("name", "$S", fullProcedureName)
                .addMember("mode", "$T.$L", Mode.class, Mode.READ)
                .build()
            )
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(Stream.class, MemoryEstimateResult.class))
            .addParameter(ParameterSpec.builder(Object.class, "graphNameOrConfiguration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "graphNameOrConfiguration").build()
                ).build()
            ).addParameter(ParameterSpec
                .builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "algoConfiguration")
                .addAnnotation(AnnotationSpec.builder(Name.class)
                    .addMember("value", "$S", "algoConfiguration").build()
                ).build())
            .addAnnotation(AnnotationSpec.builder(Description.class)
                .addMember("value", "$T.ESTIMATE_DESCRIPTION", BaseProc.class)
                .build()
            )
            .addStatement("var specification = new $T()", typeNames.specification(gdsMode))
            .addStatement("var executor = new $T<>(specification, executionContext(), transactionContext())", MemoryEstimationExecutor.class)
            .addStatement("return executor.computeEstimate(graphNameOrConfiguration, algoConfiguration)")
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
            .addStatement(
                "return $T.ensureIndexValidation(executionContext.log(), executionContext.taskRegistryFactory())",
                PregelCompanion.class
            )
            .build();
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
