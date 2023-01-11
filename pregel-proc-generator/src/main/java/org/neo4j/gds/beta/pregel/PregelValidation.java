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

import com.google.auto.common.MoreElements;
import com.google.auto.common.MoreTypes;
import com.squareup.javapoet.TypeName;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;
import org.neo4j.gds.core.CypherMapWrapper;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Optional;

import static java.util.function.Predicate.not;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class PregelValidation {

    private final Messager messager;
    private final Types typeUtils;
    private final Elements elementUtils;

    // Represents the PregelComputation interface
    private final TypeMirror pregelComputation;
    // Represents the PregelProcedureConfig interface
    private final TypeMirror pregelProcedureConfig;

    PregelValidation(Messager messager, Elements elementUtils, Types typeUtils) {
        this.messager = messager;
        this.typeUtils = typeUtils;
        this.elementUtils = elementUtils;
        this.pregelComputation = MoreTypes.asDeclared(
            typeUtils.erasure(elementUtils.getTypeElement(BasePregelComputation.class.getName()).asType())
        );
        this.pregelProcedureConfig = MoreTypes.asDeclared(elementUtils
            .getTypeElement(PregelProcedureConfig.class.getName())
            .asType());
    }

    Optional<Spec> validate(Element pregelElement) {
        if (
            !isClass(pregelElement) ||
            !isPregelComputation(pregelElement) ||
            !isPregelProcedureConfig(pregelElement) ||
            !hasEmptyConstructor(pregelElement) ||
            !configHasFactoryMethod(pregelElement)
        ) {
            return Optional.empty();
        }

        // is never null since this is the annotation that triggers the processor
        var procedure = pregelElement.getAnnotation(PregelProcedure.class);

        var computationName = pregelElement.getSimpleName().toString();
        var configTypeName = TypeName.get(config(pregelElement));
        var rootPackage = elementUtils.getPackageOf(pregelElement).getQualifiedName().toString();
        var maybeDescription = Optional.of(procedure.description()).filter(not(String::isBlank));

        return Optional.of(ImmutableSpec.of(
            pregelElement,
            computationName,
            rootPackage,
            configTypeName,
            procedure.name(),
            procedure.modes(),
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

    private boolean isPregelProcedureConfig(Element pregelElement) {
        var config = config(pregelElement);

        boolean isPregelProcedureConfig = typeUtils.isSubtype(config, pregelProcedureConfig);

        if (!isPregelProcedureConfig) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated Pregel computation must have a configuration type which is a subtype of PregelProcedureConfiguration.",
                pregelElement
            );
        }

        return isPregelProcedureConfig;
    }

    private boolean hasEmptyConstructor(Element pregelElement) {
        var pregelTypeElement = MoreElements.asType(pregelElement);
        var constructors = ElementFilter.constructorsIn(pregelElement.getEnclosedElements());

        var hasDefaultConstructor = constructors.isEmpty() || constructors
            .stream()
            .anyMatch(constructor -> constructor.getParameters().isEmpty());

        if (!hasDefaultConstructor) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated Pregel computation must have an empty constructor.",
                pregelTypeElement
            );
        }
        return hasDefaultConstructor;
    }

    private boolean configHasFactoryMethod(Element pregelElement) {
        var config = config(pregelElement);

        var cypherMapWrapperType = elementUtils.getTypeElement(CypherMapWrapper.class.getName()).asType();

        var configElement = typeUtils.asElement(config);
        var maybeHasFactoryMethod = ElementFilter.methodsIn(configElement.getEnclosedElements()).stream()
            .filter(method -> method.getModifiers().contains(Modifier.STATIC))
            .filter(method -> method.getSimpleName().contentEquals("of"))
            .filter(method -> method.getParameters().size() == 1)
            .filter(method -> typeUtils.isSameType(method.getReturnType(), config))
            .map(ExecutableElement::getParameters)
            .anyMatch(parameters ->
                typeUtils.isSameType(cypherMapWrapperType, parameters.get(0).asType())
            );

        if (!maybeHasFactoryMethod) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                formatWithLocale(
                    "Missing method 'static %s of(%s userConfig)' in %s.",
                    configElement,
                    cypherMapWrapperType,
                    configElement
                ),
                MoreElements.asType(pregelElement)
            );
        }

        return maybeHasFactoryMethod;
    }

    private TypeMirror config(Element pregelElement) {
        return pregelComputation(pregelElement)
            .map(declaredType -> declaredType.getTypeArguments().get(0))
            .orElseThrow(() -> new IllegalStateException("Could not find a pregel computation"));
    }

    @ValueClass
    interface Spec {
        Element element();

        String computationName();

        String rootPackage();

        TypeName configTypeName();

        String procedureName();

        GDSMode[] procedureModes();

        Optional<String> description();
    }

}
