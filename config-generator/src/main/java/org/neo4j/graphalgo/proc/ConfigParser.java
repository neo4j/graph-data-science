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
package org.neo4j.graphalgo.proc;

import com.google.common.collect.Streams;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.TypeName;
import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration.Ignore;
import org.neo4j.graphalgo.annotation.Configuration.Key;
import org.neo4j.graphalgo.annotation.Configuration.Parameter;
import org.neo4j.graphalgo.annotation.ValueClass;

import javax.annotation.processing.Messager;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
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

    ConfigParser(Messager messager) {
        this.messager = messager;
    }

    Spec process(TypeMirror configType) {
        TypeElement configElement = asTypeElement(configType);
        ImmutableSpec.Builder config = ImmutableSpec.builder().root(configElement).rootType(configType);
        process(config, new HashSet<>(), configElement, configElement);
        return config.build();
    }

    private void process(ImmutableSpec.Builder output, Set<String> seen, TypeElement configElement, TypeElement root) {
        methodsIn(configElement.getEnclosedElements())
            .stream()
            .map(m -> validateMember(seen, root, m))
            .flatMap(Streams::stream)
            .forEach(output::addMember);

        for (TypeMirror implemented : configElement.getInterfaces()) {
            process(output, seen, asTypeElement(implemented), root);
        }
    }

    private Optional<Member> validateMember(Collection<String> seen, TypeElement root, ExecutableElement method) {
        if (isAnnotationPresent(method, Ignore.class)) {
            seen.add(method.getSimpleName().toString());
            return Optional.empty();
        }
        if (!seen.add(method.getSimpleName().toString())) {
            return Optional.empty();
        }

        Set<Modifier> modifiers = method.getModifiers();
        if (modifiers.contains(Modifier.STATIC)) {
            return Optional.empty();
        }

        if (!method.getParameters().isEmpty()) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Method may not have any parameters",
                method
            );
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

        Key key = method.getAnnotation(Key.class);
        if (key != null) {
            if (isAnnotationPresent(method, Parameter.class)) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    String.format("The `@%s` annotation cannot be used together with the `@%s` annotation", Parameter.class.getSimpleName(), Key.class.getSimpleName()),
                    method
                );
                return Optional.empty();
            }
            memberBuilder.lookupKey(key.value());
        }

        try {
            return Optional.of(memberBuilder.build());
        } catch (InvalidMemberException invalid) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                invalid.getMessage(),
                method
            );
            return Optional.empty();
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
