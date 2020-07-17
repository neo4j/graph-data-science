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
import com.google.auto.common.MoreElements;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.graphalgo.beta.pregel.annotation.Pregel;
import org.neo4j.graphalgo.beta.pregel.annotation.Procedure;
import org.neo4j.procedure.Description;

import javax.annotation.processing.Messager;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Map;
import java.util.Optional;

public class PregelGenerator {

    private final Messager messager;
    private final Elements elementUtils;
    private final Types typeUtils;
    private final SourceVersion sourceVersion;

    public PregelGenerator(
        Messager messager,
        Elements elementUtils,
        Types typeUtils,
        SourceVersion sourceVersion
    ) {
        this.messager = messager;
        this.elementUtils = elementUtils;
        this.typeUtils = typeUtils;
        this.sourceVersion = sourceVersion;
    }

    JavaFile process(Element pregelElement) {
        var pregel = MoreElements.getAnnotationMirror(pregelElement, Pregel.class).get();

        var maybeValue = getAnnotationValue(pregel, "value");
        var maybeConfigClass = getAnnotationValue(pregel, "configClass");

        // TODO: move to validation
        if (maybeValue.isPresent() && maybeConfigClass.isPresent()) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Only one of `value` or `configClass` may be set.",
                pregelElement,
                pregel
            );
        }

        var maybeProcedure = Optional.ofNullable(pregelElement.getAnnotation(Procedure.class));

        // TODO: move to validation
        if (maybeProcedure.isEmpty()) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Procedure annotation must be present.",
                pregelElement,
                pregel
            );
            return null;
        }

        // Pregel configuration name
        // TODO: default
        var configName = maybeValue.or(() -> maybeConfigClass).get().getValue();

        // @Procedure
        var procedureName = maybeProcedure.get().value();

        // @Description
        var maybeDescription = Optional.ofNullable(MoreElements.getAnnotationMirror(pregelElement, Description.class).orNull());

        return generate(pregelElement, configName, procedureName, maybeDescription);
    }

    private JavaFile generate(Element pregelElement, AnnotationValue configName, String procedureName, Optional<AnnotationMirror> maybeDescription) {
        var rootPackage = elementUtils.getPackageOf(pregelElement);
        String packageName = rootPackage.getQualifiedName().toString();
        TypeSpec typeSpec = process(pregelElement, configName, procedureName, maybeDescription, packageName);
        return JavaFile
            .builder(packageName, typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
    }

    private TypeSpec process(Element pregelElement, AnnotationValue configName, String procedureName, Optional<AnnotationMirror> maybeDescription, String packageName) {
        var typeSpecBuilder = TypeSpec
            .classBuilder(ClassName.get(packageName, "Foo"))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addOriginatingElement(pregelElement);

        // produces @Generated meta info
        GeneratedAnnotationSpecs.generatedAnnotationSpec(
            elementUtils,
            sourceVersion,
            PregelProcessor.class
        ).ifPresent(typeSpecBuilder::addAnnotation);

        // add proc stream method
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("stream");
        // add description
        maybeDescription.ifPresent(annotationMirror -> methodBuilder.addAnnotation(AnnotationSpec.get(annotationMirror)));
        // add procedure annotation
        methodBuilder.addAnnotation(AnnotationSpec.builder(org.neo4j.procedure.Procedure.class)
            .addMember("name", "$S", procedureName)
            .build()
        );
        var method = methodBuilder
            .addModifiers(Modifier.PUBLIC)
            .build();

        return typeSpecBuilder
            .addMethod(method)
            .build();
    }

    private Optional<Map.Entry<ExecutableElement, AnnotationValue>> getAnnotationValue(
        AnnotationMirror annotation,
        String elementName
    ) {
        var declaredValues = annotation.getElementValues();

        for (var method : ElementFilter.methodsIn(annotation.getAnnotationType().asElement().getEnclosedElements())) {
            if (method.getSimpleName().contentEquals(elementName)) {
                return declaredValues.containsKey(method)
                    ? Optional.of(Map.entry(method, declaredValues.get(method)))
                    : Optional.empty();
            }
        }
        return Optional.empty();
    }
}
