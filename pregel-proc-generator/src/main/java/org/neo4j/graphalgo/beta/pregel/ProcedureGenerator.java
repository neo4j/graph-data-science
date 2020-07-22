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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.logging.Log;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.Optional;

abstract class ProcedureGenerator extends PregelGenerator {

    ProcedureGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        super(elementUtils, sourceVersion);
    }

    static TypeSpec forMode(
        org.neo4j.graphalgo.beta.pregel.annotation.Mode mode,
        Elements elementUtils,
        SourceVersion sourceVersion,
        PregelValidation.Spec pregelSpec
    ) {
        switch (mode) {
            case STREAM: return new StreamProcedureGenerator(elementUtils, sourceVersion).typeSpec(pregelSpec);
            case WRITE:
            case MUTATE:
            case STATS:
            default: throw new IllegalArgumentException("Unsupported Mode " + mode);
        }
    }

    abstract String procClassInfix();

    abstract Class<?> procBaseClass();

    abstract Class<?> procResultClass();

    abstract MethodSpec procMethod(PregelValidation.Spec pregelSpec);

    abstract MethodSpec procResultMethod(PregelValidation.Spec pregelSpec);

    TypeSpec typeSpec(PregelValidation.Spec pregelSpec) {
        var configTypeName = pregelSpec.configTypeName();
        var procedureClassName = className(pregelSpec, procClassInfix() + PROCEDURE_SUFFIX);
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

        typeSpecBuilder.addMethod(procMethod(pregelSpec));
        typeSpecBuilder.addMethod(procResultMethod(pregelSpec));

        typeSpecBuilder.addMethod(newConfigMethod(pregelSpec));
        typeSpecBuilder.addMethod(algorithmFactoryMethod(pregelSpec, algorithmClassName));
        typeSpecBuilder.addMethod(propertyTranslator(pregelSpec, algorithmClassName));

        return typeSpecBuilder.build();
    }

    private MethodSpec newConfigMethod(PregelValidation.Spec pregelSpec) {
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

    private MethodSpec algorithmFactoryMethod(PregelValidation.Spec pregelSpec, ClassName algorithmClassName) {
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

    private MethodSpec propertyTranslator(PregelValidation.Spec pregelSpec, ClassName algorithmClassName) {
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
