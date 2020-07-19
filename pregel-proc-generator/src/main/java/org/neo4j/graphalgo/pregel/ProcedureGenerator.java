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
package org.neo4j.graphalgo.pregel;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.beta.pregel.PregelResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

class ProcedureGenerator extends PregelGenerator {

    ProcedureGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        super(elementUtils, sourceVersion);
    }

    TypeSpec typeSpec(PregelValidation.Spec pregelSpec) {
        TypeName configTypeName = configTypeName(pregelSpec);
        ClassName procedureClassName = className(pregelSpec, PROCEDURE_SUFFIX);
        ClassName algorithmClassName = className(pregelSpec, ALGORITHM_SUFFIX);

        var typeSpecBuilder = TypeSpec
            .classBuilder(procedureClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(StreamProc.class),
                algorithmClassName,
                ClassName.get(HugeDoubleArray.class),
                ClassName.get(PregelResult.class),
                configTypeName
            ))
            .addOriginatingElement(pregelSpec.element());

        addGeneratedAnnotation(typeSpecBuilder);

        typeSpecBuilder.addMethod(streamMethod(pregelSpec));
        typeSpecBuilder.addMethod(streamResultMethod());
        typeSpecBuilder.addMethod(newConfigMethod(pregelSpec));
        typeSpecBuilder.addMethod(algorithmFactoryMethod(pregelSpec, algorithmClassName));

        return typeSpecBuilder.build();
    }

    private MethodSpec streamMethod(PregelValidation.Spec pregelSpec) {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("stream");

        // add procedure annotation
        methodBuilder.addAnnotation(AnnotationSpec.builder(org.neo4j.procedure.Procedure.class)
            .addMember("name", "$S", pregelSpec.procedureName())
            .addMember("mode", "$T.READ", Mode.class)
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
            .addStatement("return stream(compute(graphNameOrConfig, configuration))")
            .returns(ParameterizedTypeName.get(Stream.class, PregelResult.class))
            .build();
    }

    private MethodSpec streamResultMethod() {
        return MethodSpec.methodBuilder("streamResult")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(PregelResult.class)
            .addParameter(long.class, "originalNodeId")
            .addParameter(double.class, "value")
            .addStatement("return new PregelResult(originalNodeId, value)")
            .build();
    }

    private MethodSpec newConfigMethod(PregelValidation.Spec pregelSpec) {
        return MethodSpec.methodBuilder("newConfig")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .addParameter(String.class, "username")
            .addParameter(ParameterizedTypeName.get(Optional.class, String.class), "graphName")
            .addParameter(ParameterizedTypeName.get(Optional.class, GraphCreateConfig.class), "maybeImplicitCreate")
            .addParameter(CypherMapWrapper.class, "config")
            .returns(configTypeName(pregelSpec))
            // TODO: create config statement
            .addStatement("return null")
            .build();
    }

    private MethodSpec algorithmFactoryMethod(PregelValidation.Spec pregelSpec, ClassName algorithmClassName) {
        return MethodSpec.methodBuilder("algorithmFactory")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(ParameterizedTypeName.get(
                ClassName.get(AlgorithmFactory.class),
                algorithmClassName,
                configTypeName(pregelSpec)
            ))
            // TODO instantiate factory
            .addStatement("return null")
            .build();
    }

}
