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

import com.google.auto.common.MoreElements;
import com.google.auto.common.MoreTypes;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.annotation.Pregel;
import org.neo4j.graphalgo.beta.pregel.annotation.Procedure;
import org.neo4j.procedure.Description;

import javax.annotation.processing.Messager;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Optional;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class PregelValidation {

    private final Messager messager;
    private final Types typeUtils;
    private final Elements elementUtils;

    // Represents the PregelComputation interface
    private final TypeMirror pregelComputation;

    PregelValidation(Messager messager, Elements elementUtils, Types typeUtils) {
        this.messager = messager;
        this.typeUtils = typeUtils;
        this.elementUtils = elementUtils;
        this.pregelComputation = MoreTypes.asDeclared(
            typeUtils.erasure(elementUtils.getTypeElement(PregelComputation.class.getName()).asType())
        );
    }

    Optional<Spec> validate(Element pregelElement) {
        var pregelAnnotationMirror = MoreElements.getAnnotationMirror(pregelElement, Pregel.class).get();
        var maybeProcedure = Optional.ofNullable(pregelElement.getAnnotation(Procedure.class));

        if (
            !isClass(pregelElement) ||
            !isPregelComputation(pregelElement) ||
            !hasProcedureAnnotation(maybeProcedure, pregelElement, pregelAnnotationMirror)
            // TODO: validate that config has a factory method with correct signature
        ) {
            return Optional.empty();
        }

        var computationName = pregelElement.getSimpleName().toString();
        var configTypeName = TypeName.get(config(pregelElement));
        var rootPackage = elementUtils.getPackageOf(pregelElement).getQualifiedName().toString();
        var maybeDescription = Optional.ofNullable(MoreElements
            .getAnnotationMirror(pregelElement, Description.class)
            .orNull());

        return Optional.of(ImmutableSpec.of(
            pregelElement,
            computationName,
            rootPackage,
            configTypeName,
            maybeProcedure.get().value(),
            maybeDescription
        ));
    }

    private boolean isClass(Element pregelElement) {
        boolean isClass = pregelElement.getKind() == ElementKind.CLASS;
        if (!isClass) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated Pregel computation must be a class.",
                pregelElement
            );
        }
        return isClass;
    }

    private Optional<DeclaredType> pregelComputation(Element pregelElement) {
        // TODO: this check needs to bubble up the inheritance tree
        return MoreElements.asType(pregelElement).getInterfaces().stream()
            .map(MoreTypes::asDeclared)
            .filter(declaredType -> typeUtils.isSubtype(declaredType, pregelComputation))
            .findFirst();
    }

    private boolean isPregelComputation(Element pregelElement) {
        var pregelTypeElement = MoreElements.asType(pregelElement);
        var maybeInterface = pregelComputation(pregelElement);
        boolean isPregelComputation = maybeInterface.isPresent();

        if (!isPregelComputation) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated Pregel computation must implement the PregelComputation interface.",
                pregelTypeElement
            );
        }
        return isPregelComputation;
    }

    private TypeMirror config(Element pregelElement) {
        var maybeInterface = pregelComputation(pregelElement);
        return maybeInterface.get()
            .getTypeArguments()
            .stream()
            .findFirst()
            .get();
    }

    private boolean hasProcedureAnnotation(
        Optional<Procedure> maybeProcedure,
        Element pregelElement,
        AnnotationMirror pregelAnnotationMirror
    ) {
        return maybeProcedure.map(x -> true).orElseGet(() -> {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated Pregel computation must be annotated with the @Procedure annotation.",
                pregelElement,
                pregelAnnotationMirror
            );
            return false;
        });
    }

    @ValueClass
    interface Spec {
        Element element();

        String computationName();

        String rootPackage();

        TypeName configTypeName();

        String procedureName();

        Optional<AnnotationMirror> description();
    }

}
