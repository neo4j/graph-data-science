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

import com.google.common.collect.Streams;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.TypeName;
import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.Configuration.CollectKeys;
import org.neo4j.gds.annotation.Configuration.Ignore;
import org.neo4j.gds.annotation.Configuration.Key;
import org.neo4j.gds.annotation.Configuration.Parameter;
import org.neo4j.gds.annotation.Configuration.ToMap;
import org.neo4j.gds.annotation.ValueClass;

import javax.annotation.processing.Messager;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auto.common.MoreElements.asType;
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
        ImmutableSpec.Builder config = ImmutableSpec.builder().root(configElement).rootType(configType);
        process(config, new HashSet<>(), configElement, configElement);
        return config.build();
    }

    private void process(ImmutableSpec.Builder output, Set<String> seen, TypeElement configElement, TypeElement root) {
        var members = methodsIn(configElement.getEnclosedElements())
            .stream()
            .map(m -> validateMember(seen, root, m))
            .flatMap(Streams::stream)
            .map(this::validateParameters)
            .flatMap(Streams::stream)
            .collect(Collectors.toList());

        if (members.stream().filter(Member::graphStoreValidation).count() > 1) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "[ConfigParser]: Only one GraphStoreValidation-annotated method allowed"
            );
            return;
        }

        members.forEach(output::addMember);

        for (TypeMirror implemented : configElement.getInterfaces()) {
            process(output, seen, asTypeElement(implemented), root);
        }
    }

    private Optional<Member> validateParameters(Member member) {
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

    private Optional<Member> validateMember(Collection<String> seen, TypeElement root, ExecutableElement method) {
        if (isAnnotationPresent(method, Ignore.class)) {
            seen.add(method.getSimpleName().toString());
            return Optional.empty();
        }
        if (!seen.add(method.getSimpleName().toString()) || method.getModifiers().contains(Modifier.STATIC)) {
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

        ImmutableMember.Builder memberBuilder = ImmutableMember
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
                    String.format(Locale.ENGLISH,"The `@%s` annotation cannot be used together with the `@%s` annotation", Parameter.class.getSimpleName(), Key.class.getSimpleName()),
                    method
                );
                return Optional.empty();
            }
            memberBuilder.lookupKey(key.value());
        }

        try {
            return Optional.of(memberBuilder.build());
        } catch (InvalidMemberException invalid) {
            messager.printMessage(Diagnostic.Kind.ERROR, invalid.getMessage(), method);
            return Optional.empty();
        }
    }

    private void validateToMap(ExecutableElement method, ImmutableMember.Builder memberBuilder) {
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

    private void validateCollectKeys(ExecutableElement method, ImmutableMember.Builder memberBuilder) {
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

    private void validateGraphStoreValidationAndChecks(ExecutableElement method, ImmutableMember.Builder memberBuilder) {
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

    private void validateValueCheck(ExecutableElement method, ImmutableMember.Builder memberBuilder) {
        if (isAnnotationPresent(method, Value.Check.class)) {
            if (method.getReturnType().getKind() == TypeKind.VOID) {
                memberBuilder.validates(true);
            } else {
                memberBuilder.normalizes(true);
            }
        }
    }

    @ValueClass
    interface Spec {
        TypeElement root();

        TypeMirror rootType();

        List<Member> members();
    }

    @ValueClass
    abstract static class Member {
        public abstract TypeElement owner();

        public abstract ExecutableElement method();

        @Value.Default
        public String lookupKey() {
            return methodName();
        }

        @Value.Default
        public boolean collectsKeys() {
            return false;
        }

        @Value.Default
        public boolean toMap() {
            return false;
        }

        @Value.Default
        public boolean validatesIntegerRange() { return false; }

        @Value.Default
        public boolean validatesLongRange() { return false; }

        @Value.Default
        public boolean validatesDoubleRange() { return false; }

        @Value.Default
        public boolean validates() {
            return false;
        }

        @Value.Default
        public boolean graphStoreValidation() { return false; }

        @Value.Default
        public boolean graphStoreValidationCheck() { return false; }

        @Value.Default
        public boolean normalizes() {
            return false;
        }

        final boolean isConfigValue() {
            return !collectsKeys() && !toMap() && !validates() && !normalizes() && !graphStoreValidation() && !graphStoreValidationCheck();
        }

        final boolean isMapParameter() {
            return isConfigValue() && !isAnnotationPresent(method(), Parameter.class);
        }

        @Value.Derived
        public String methodName() {
            return method().getSimpleName().toString();
        }

        public Set<AnnotationMirror> annotations(Class<? extends Annotation> annotationType) {
            return Stream.concat(
                method().getReturnType().getAnnotationMirrors().stream(),
                method().getAnnotationMirrors().stream()
            )
                .filter(am ->
                    asType(am.getAnnotationType().asElement())
                        .getQualifiedName()
                        .contentEquals(annotationType.getName()))
                .collect(Collectors.toCollection(() -> new TreeSet<>(
                    Comparator.comparing(am -> asType(am.getAnnotationType().asElement()).getQualifiedName().toString())
                )));
        }

        public TypeName typeSpecWithAnnotation(Class<? extends Annotation> annotationType) {
            Set<AnnotationMirror> annotations = annotations(annotationType);
            TypeName typeName = TypeName.get(method().getReturnType());
            List<AnnotationSpec> annotationsToAddToType = annotations
                .stream()
                .map(AnnotationSpec::get)
                .collect(Collectors.toList());
            return typeName.annotated(annotationsToAddToType);
        }

        @Value.Check
        final Member normalize() {
            String trimmedKey = lookupKey().trim();
            if (trimmedKey.isEmpty()) {
                throw new InvalidMemberException("The key must not be empty");
            }
            if (collectsKeys() && (validates() || normalizes())) {
                throw new InvalidMemberException(String.format(
                    Locale.ENGLISH,
                    "Cannot combine @%s with @%s",
                    CollectKeys.class.getSimpleName(),
                    Value.Check.class.getSimpleName()
                ));
            }
            if (toMap() && (validates() || normalizes())) {
                throw new InvalidMemberException(String.format(
                    Locale.ENGLISH,
                    "Cannot combine @%s with @%s",
                    ToMap.class.getSimpleName(),
                    Value.Check.class.getSimpleName()
                ));
            }
            if (trimmedKey.equals(lookupKey())) {
                return this;
            }
            return ((ImmutableMember) this).withLookupKey(trimmedKey);
        }
    }

    private static final class InvalidMemberException extends RuntimeException {
        InvalidMemberException(String message) {
            super(message);
        }
    }
}
