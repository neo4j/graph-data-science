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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.TypeName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.GenerateBuilder;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import java.lang.annotation.Annotation;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auto.common.MoreElements.asType;
import static com.google.auto.common.MoreElements.isAnnotationPresent;

@GenerateBuilder
record Spec(TypeElement root, TypeMirror rootType, List<Member> members) {

    @GenerateBuilder
    record Member(
        @NotNull TypeElement owner,
        @NotNull ExecutableElement method,
        @Nullable String lookupKey,
        boolean collectsKeys,
        boolean toMap,
        boolean validatesIntegerRange,
        boolean validatesLongRange,
        boolean validatesDoubleRange,
        boolean validates,
        boolean graphStoreValidation,
        boolean graphStoreValidationCheck,
        boolean normalizes
    ) {
        Member {
            var methodName = method.getSimpleName().toString();
            var resolvedKey = Objects.requireNonNullElse(lookupKey, methodName);
            var trimmedKey = resolvedKey.trim();
            if (trimmedKey.isEmpty()) {
                throw new InvalidMemberException("The key must not be empty");
            }
            if (!trimmedKey.equals(resolvedKey)) {
                throw new InvalidMemberException("The key must not be surrounded by whitespace");
            }
            if (collectsKeys && (validates || normalizes)) {
                throw new InvalidMemberException(String.format(
                    Locale.ENGLISH,
                    "Cannot combine @%s with @%s",
                    Configuration.CollectKeys.class.getSimpleName(),
                    Configuration.Check.class.getSimpleName()
                ));
            }
            if (toMap && (validates || normalizes)) {
                throw new InvalidMemberException(String.format(
                    Locale.ENGLISH,
                    "Cannot combine @%s with @%s",
                    Configuration.ToMap.class.getSimpleName(),
                    Configuration.Check.class.getSimpleName()
                ));
            }
        }

        public String lookupKey() {
            return Objects.requireNonNullElseGet(lookupKey, this::methodName);
        }

        boolean isConfigValue() {
            return !collectsKeys() && !toMap() && !validates() && !normalizes() && !graphStoreValidation() && !graphStoreValidationCheck();
        }

        boolean isConfigMapEntry() {
            return isConfigValue() && !isAnnotationPresent(method(), Configuration.Parameter.class);
        }

        boolean isConfigParameter() {
            return isConfigValue() && isAnnotationPresent(method(), Configuration.Parameter.class);
        }

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
    }

    static final class InvalidMemberException extends RuntimeException {
        InvalidMemberException(String message) {
            super(message);
        }
    }
}
