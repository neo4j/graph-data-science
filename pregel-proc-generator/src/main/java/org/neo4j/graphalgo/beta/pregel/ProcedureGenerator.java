/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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
        var procedureClassName = className(pregelSpec, procGdsMode().camelCase() + PROCEDURE_SUFFIX);
        var algorithmClassName = className(pregelSpec, ALGORITHM_SUFFIX);

        var typeSpecBuilder = TypeSpec
            .classBuilder(procedureClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(procBaseClass()),
                algorithmClassName,
                ClassName.get(HugeDoubleArray.class),
                ClassName.get(procResultClass()),
                configTypeName
            ))
            .addOriginatingElement(pregelSpec.element());

        addGeneratedAnnotation(typeSpecBuilder);

        typeSpecBuilder.addMethod(procMethod());
        typeSpecBuilder.addMethod(procResultMethod());

        typeSpecBuilder.addMethod(newConfigMethod());
        typeSpecBuilder.addMethod(algorithmFactoryMethod(algorithmClassName));
        typeSpecBuilder.addMethod(propertyTranslator(algorithmClassName));

        return typeSpecBuilder.build();
    }

    private MethodSpec procMethod() {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(procGdsMode().lowerCase());

        // add procedure annotation
        methodBuilder.addAnnotation(AnnotationSpec.builder(org.neo4j.procedure.Procedure.class)
            .addMember("name", "$S", pregelSpec.procedureName() + "." + procGdsMode().lowerCase())
            .addMember("mode", "$T.$L", org.neo4j.procedure.Mode.class, procExecMode())
            .build()
        );
        // add description
        pregelSpec.description().ifPresent(annotationMirror -> methodBuilder.addAnnotation(AnnotationSpec.get(annotationMirror)));

        return methodBuilder
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(Object.class, "graphNameOrConfig")
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
                .build())
            .addStatement("return $L(compute(graphNameOrConfig, configuration))", procGdsMode().lowerCase())
            .returns(ParameterizedTypeName.get(Stream.class, procResultClass()))
            .build();
    }

    private MethodSpec newConfigMethod() {
        return MethodSpec.methodBuilder("newConfig")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .addParameter(String.class, "username")
            .addParameter(ParameterizedTypeName.get(Optional.class, String.class), "graphName")
            .addParameter(ParameterizedTypeName.get(Optional.class, GraphCreateConfig.class), "maybeImplicitCreate")
            .addParameter(CypherMapWrapper.class, "config")
            .returns(pregelSpec.configTypeName())
            .addStatement("return $T.of(username, graphName, maybeImplicitCreate, config)", pregelSpec.configTypeName())
            .build();
    }

    private MethodSpec algorithmFactoryMethod(ClassName algorithmClassName) {
        TypeSpec anonymousFactoryType = TypeSpec.anonymousClassBuilder("")
            .addSuperinterface(ParameterizedTypeName.get(
                ClassName.get(AlgorithmFactory.class),
                algorithmClassName,
                pregelSpec.configTypeName()
            ))
            .addMethod(MethodSpec.methodBuilder("build")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(Graph.class, "graph")
                .addParameter(pregelSpec.configTypeName(), "configuration")
                .addParameter(AllocationTracker.class, "tracker")
                .addParameter(Log.class, "log")
                .returns(algorithmClassName)
                .addStatement("return new $T(graph, configuration, tracker, log)", algorithmClassName)
                .build()
            )
            .addMethod(MethodSpec.methodBuilder("memoryEstimation")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(MemoryEstimation.class)
                .addParameter(pregelSpec.configTypeName(), "configuration")
                .addStatement("return $T.empty()", MemoryEstimations.class)
                .build()
            )
            .build();

        return MethodSpec.methodBuilder("algorithmFactory")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(ParameterizedTypeName.get(
                ClassName.get(AlgorithmFactory.class),
                algorithmClassName,
                pregelSpec.configTypeName()
            ))
            .addStatement("return $L", anonymousFactoryType)
            .build();
    }

    private MethodSpec propertyTranslator(ClassName algorithmClassName) {
        return MethodSpec.methodBuilder("nodePropertyTranslator")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(ParameterizedTypeName.get(PropertyTranslator.class, HugeDoubleArray.class))
            .addParameter(ParameterizedTypeName.get(
                ClassName.get(AlgoBaseProc.ComputationResult.class),
                algorithmClassName,
                ClassName.get(HugeDoubleArray.class),
                pregelSpec.configTypeName()
                ), "computationResult"
            )
            .addStatement("return $T.Translator.INSTANCE", HugeDoubleArray.class)
            .build();
    }

}
