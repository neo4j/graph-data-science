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

import com.google.auto.common.GeneratedAnnotationSpecs;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.beta.pregel.PregelResult;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleAnnotationValueVisitor9;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class PregelGenerator {

    private final Elements elementUtils;
    private final SourceVersion sourceVersion;

    PregelGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        this.elementUtils = elementUtils;
        this.sourceVersion = sourceVersion;
    }

    List<JavaFile> process(PregelValidation.Spec pregelSpec) {
        return List.of(
            fileOf(pregelSpec, procedureTypeSpec(pregelSpec))
        );
    }

    private JavaFile fileOf(PregelValidation.Spec pregelSpec, TypeSpec typeSpec) {
        return JavaFile
            .builder(pregelSpec.rootPackage(), typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }

    private TypeSpec procedureTypeSpec(PregelValidation.Spec pregelSpec) {
        TypeName configTypeName = configTypeName(pregelSpec);

        var typeSpecBuilder = TypeSpec
            .classBuilder(ClassName.get(pregelSpec.rootPackage(), pregelSpec.computationName() + "StreamProc"))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ParameterizedTypeName.get(
                ClassName.get(StreamProc.class),
                ClassName.get(pregelSpec.rootPackage(), pregelSpec.computationName() + "Algorithm"),
                ClassName.get(HugeDoubleArray.class),
                ClassName.get(PregelResult.class),
                configTypeName
            ))
            .addOriginatingElement(pregelSpec.element());

        // produces @Generated meta info
        GeneratedAnnotationSpecs.generatedAnnotationSpec(
            elementUtils,
            sourceVersion,
            PregelProcessor.class
        ).ifPresent(typeSpecBuilder::addAnnotation);

        // add proc stream method
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("stream");
        // add procedure annotation
        methodBuilder.addAnnotation(AnnotationSpec.builder(org.neo4j.procedure.Procedure.class)
            .addMember("name", "$S", pregelSpec.procedureName())
            .addMember("mode", "$T.READ", Mode.class)
            .build()
        );
        // add description
        pregelSpec.description().ifPresent(annotationMirror -> methodBuilder.addAnnotation(AnnotationSpec.get(annotationMirror)));

        var method = methodBuilder
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

        return typeSpecBuilder
            .addMethod(method)
            .build();
    }

    private TypeName configTypeName(PregelValidation.Spec pregelSpec) {
        return pregelSpec.configName()
                .accept(new SimpleAnnotationValueVisitor9<TypeName, Void>() {
                    @Override
                    public TypeName visitType(TypeMirror t, Void aVoid) {
                        return TypeName.get(t);
                    }
                }, null);
    }
}
