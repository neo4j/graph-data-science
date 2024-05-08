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
package org.neo4j.gds.proc;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.Configuration.CollectKeys;
import org.neo4j.gds.annotation.Configuration.Ignore;
import org.neo4j.gds.annotation.Configuration.Key;
import org.neo4j.gds.annotation.Configuration.Parameter;
import org.neo4j.gds.annotation.Configuration.ToMap;

import javax.annotation.processing.Messager;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.auto.common.MoreElements.isAnnotationPresent;
import static com.google.auto.common.MoreTypes.asTypeElement;
import static javax.lang.model.util.ElementFilter.methodsIn;

final class ConfigParser {

    private final Messager messager;
    private final Elements elementUtils;
    private final Types typeUtils;

    ConfigParser(Messager messager, Elements elementUtils, Types typeUtils) {
        this.messager = messager;
        this.elementUtils = elementUtils;
        this.typeUtils = typeUtils;
    }

    Spec process(TypeMirror configType) {
        TypeElement configElement = asTypeElement(configType);
        SpecBuilder config = SpecBuilder.builder().root(configElement).rootType(configType);
        process(config, new HashSet<>(), configElement, configElement);
        return config.build();
    }

    private void process(
        SpecBuilder output,
        Set<ExecutableElement> seen,
        TypeElement configElement,
        TypeElement root
    ) {
        var members = methodsIn(configElement.getEnclosedElements())
            .stream()
            .map(m -> validateMember(seen, root, m))
            .flatMap(Optional::stream)
            .map(this::validateParameters)
            .flatMap(Optional::stream)
            .toList();

        if (members.stream().filter(Spec.Member::graphStoreValidation).count() > 1) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "[ConfigParser]: Only one GraphStoreValidation-annotated method allowed"
            );
            return;
        }

        members.forEach(output::addMembers);

        for (TypeMirror implemented : configElement.getInterfaces()) {
            process(output, seen, asTypeElement(implemented), root);
        }
    }

    private Optional<Spec.Member> validateParameters(Spec.Member member) {
        var method = member.method();
        if (member.graphStoreValidation() || member.graphStoreValidationCheck()) {
            if (method.getParameters().size() != 3) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "[ConfigParser]: GraphStoreValidation and Checks must accept 3 parameters",
                    method
                );
                return Optional.empty();
            }
        } else {
            if (!method.getParameters().isEmpty()) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "[ConfigParser]: Method may not have any parameters",
                    method
                );
                return Optional.empty();
            }
        }
        return Optional.of(member);
    }

    private Optional<Spec.Member> validateMember(
        Set<ExecutableElement> seenMembers,
        TypeElement root,
        ExecutableElement method
    ) {
        var seenMembersWithSameName = seenMembers.stream().filter(m -> m.getSimpleName()
            .equals(method.getSimpleName()));

        // check Ignore corner cases
        if (isAnnotationPresent(method, Ignore.class)) {
            var invalidRelatedMember = seenMembersWithSameName
                .filter(m -> !(methodMarkedAsInput(m) || isAnnotationPresent(m, Ignore.class)))
                .findAny();

            invalidRelatedMember.ifPresent(relatedMember -> messager.printMessage(
                Diagnostic.Kind.ERROR,
                String.format(
                    Locale.ENGLISH,
                    "Method `%1$s` annotated with `%2$s` cannot override without explicit clarifying the parent method by using the `%2$s`, `%3$s` or `%4$s` annotation.",
                    method.getSimpleName(),
                    Ignore.class.getSimpleName(),
                    Parameter.class.getSimpleName(),
                    Key.class.getSimpleName()
                ),
                method
            ));

            seenMembers.add(method);

            return Optional.empty();
        }

        var seenMembersList = seenMembersWithSameName.toList();
        if (!seenMembersList.isEmpty()) {
            var anyRelatedIgnoredSeenMember = seenMembersList.stream().anyMatch(m -> isAnnotationPresent(
                m,
                Ignore.class
            ));
            if (anyRelatedIgnoredSeenMember && !methodMarkedAsInput(method)) {
                // for inheritance must clarify the status of the member to avoid exposing ignored fields from the parent
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    String.format(
                        Locale.ENGLISH,
                        "Method `%1$s` was previously annotated with `%2$s` but cannot be overridden without explicit clarification by using the `%2$s`, `%3$s` or `%4$s` annotation.",
                        method.getSimpleName(),
                        Ignore.class.getSimpleName(),
                        Parameter.class.getSimpleName(),
                        Key.class.getSimpleName()
                    ),
                    method
                );
            }

            seenMembers.add(method);

            return Optional.empty();
        }

        seenMembers.add(method);

        if (method.getModifiers().contains(Modifier.STATIC)) {
            // static method cannot be config fields
            return Optional.empty();
        }

        if (!method.getTypeParameters().isEmpty()) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Method may not have any type parameters",
                method
            );
            return Optional.empty();
        }

        if (!method.getThrownTypes().isEmpty()) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Method may not declare any exceptions to be thrown",
                method
            );
            return Optional.empty();
        }

        MemberBuilder memberBuilder = MemberBuilder
            .builder()
            .owner(root)
            .method(method);

        validateCollectKeys(method, memberBuilder);
        validateToMap(method, memberBuilder);

        memberBuilder.validatesIntegerRange(isAnnotationPresent(method, Configuration.IntegerRange.class));
        memberBuilder.validatesLongRange(isAnnotationPresent(method, Configuration.LongRange.class));
        memberBuilder.validatesDoubleRange(isAnnotationPresent(method, Configuration.DoubleRange.class));

        validateValueCheck(method, memberBuilder);

        validateGraphStoreValidationAndChecks(method, memberBuilder);

        Key key = method.getAnnotation(Key.class);
        if (key != null) {
            if (isAnnotationPresent(method, Parameter.class)) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    String.format(
                        Locale.ENGLISH,
                        "The `@%s` annotation cannot be used together with the `@%s` annotation",
                        Parameter.class.getSimpleName(),
                        Key.class.getSimpleName()
                    ),
                    method
                );
                return Optional.empty();
            }
            memberBuilder.lookupKey(key.value());
        }

        try {
            return Optional.of(memberBuilder.build());
        } catch (Spec.InvalidMemberException invalid) {
            messager.printMessage(Diagnostic.Kind.ERROR, invalid.getMessage(), method);
            return Optional.empty();
        }
    }

    private static boolean methodMarkedAsInput(ExecutableElement method) {
        return isAnnotationPresent(method, Key.class)
            || isAnnotationPresent(method, Parameter.class)
            || isAnnotationPresent(method, Configuration.ConvertWith.class)
            || isAnnotationPresent(method, Configuration.DoubleRange.class)
            || isAnnotationPresent(method, Configuration.IntegerRange.class);
    }

    private void validateToMap(ExecutableElement method, MemberBuilder memberBuilder) {
        if (isAnnotationPresent(method, ToMap.class)) {
            TypeElement mapType = elementUtils.getTypeElement(Map.class.getTypeName());
            TypeMirror stringType = elementUtils.getTypeElement(String.class.getTypeName()).asType();
            TypeMirror objectType = elementUtils.getTypeElement(Object.class.getTypeName()).asType();
            DeclaredType mapOfStringToObjectType = typeUtils.getDeclaredType(mapType, stringType, objectType);

            if (!typeUtils.isSameType(method.getReturnType(), mapOfStringToObjectType)) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Method must return Map<String, Object>",
                    method
                );
            }

            memberBuilder.toMap(true);
        }
    }

    private void validateCollectKeys(ExecutableElement method, MemberBuilder memberBuilder) {
        if (isAnnotationPresent(method, CollectKeys.class)) {
            TypeElement collectionType = elementUtils.getTypeElement(Collection.class.getTypeName());
            TypeMirror stringType = elementUtils.getTypeElement(String.class.getTypeName()).asType();
            DeclaredType collectionOfStringType = typeUtils.getDeclaredType(collectionType, stringType);

            if (!typeUtils.isSameType(method.getReturnType(), collectionOfStringType)) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Method must return Collection<String>",
                    method
                );
            }

            memberBuilder.collectsKeys(true);
        }
    }

    private void validateGraphStoreValidationAndChecks(
        ExecutableElement method,
        MemberBuilder memberBuilder
    ) {
        if (isAnnotationPresent(method, Configuration.GraphStoreValidation.class)) {
            requireVoidReturnType(method);
            requireDefaultModifier(method);

            memberBuilder.graphStoreValidation(true);
        }
        if (isAnnotationPresent(method, Configuration.GraphStoreValidationCheck.class)) {
            requireVoidReturnType(method);
            requireDefaultModifier(method);

            memberBuilder.graphStoreValidationCheck(true);
        }
    }

    private void requireVoidReturnType(ExecutableElement method) {
        if (!typeUtils.isSameType(method.getReturnType(), typeUtils.getNoType(TypeKind.VOID))) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "[ConfigParser]: GraphStoreValidation and Checks must return void",
                method
            );
        }
    }

    private void requireDefaultModifier(ExecutableElement method) {
        if (!method.getModifiers().contains(Modifier.DEFAULT)) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "[ConfigParser]: GraphStoreValidation and Checks must be declared default (cannot be abstract)",
                method
            );
        }
    }

    private static void validateValueCheck(ExecutableElement method, MemberBuilder memberBuilder) {
        if (isAnnotationPresent(method, Configuration.Check.class)) {
            if (method.getReturnType().getKind() == TypeKind.VOID) {
                memberBuilder.validates(true);
            } else {
                memberBuilder.normalizes(true);
            }
        }
    }

}
