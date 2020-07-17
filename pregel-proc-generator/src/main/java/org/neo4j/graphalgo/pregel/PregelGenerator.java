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
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.Elements;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class PregelGenerator {

    private final Elements elementUtils;
    private final SourceVersion sourceVersion;

    PregelGenerator(Elements elementUtils, SourceVersion sourceVersion) {
        this.elementUtils = elementUtils;
        this.sourceVersion = sourceVersion;
    }

    JavaFile process(PregelValidation.Spec pregelSpec) {
        TypeSpec typeSpec = generateTypeSpec(pregelSpec);

        return JavaFile
            .builder(pregelSpec.rootPackage(), typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }

    private TypeSpec generateTypeSpec(PregelValidation.Spec pregelSpec) {
        var typeSpecBuilder = TypeSpec
            .classBuilder(ClassName.get(pregelSpec.rootPackage(), pregelSpec.computationName() + "StreamProc"))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addOriginatingElement(pregelSpec.element());

        // produces @Generated meta info
        GeneratedAnnotationSpecs.generatedAnnotationSpec(
            elementUtils,
            sourceVersion,
            PregelProcessor.class
        ).ifPresent(typeSpecBuilder::addAnnotation);

        // add proc stream method
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("stream");
        // add description
        pregelSpec.description().ifPresent(annotationMirror -> methodBuilder.addAnnotation(AnnotationSpec.get(annotationMirror)));
        // add procedure annotation
        methodBuilder.addAnnotation(AnnotationSpec.builder(org.neo4j.procedure.Procedure.class)
            .addMember("name", "$S", pregelSpec.procedureName())
            .build()
        );
        var method = methodBuilder
            .addModifiers(Modifier.PUBLIC)
            .build();

        return typeSpecBuilder
            .addMethod(method)
            .build();
    }
}
