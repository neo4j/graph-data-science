/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration.Ignore;
import org.neo4j.graphalgo.annotation.ValueClass;

import javax.annotation.processing.Messager;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
            return Optional.empty();
        }
        if (!seen.add(method.getSimpleName().toString())) {
            return Optional.empty();
        }

        Set<Modifier> modifiers = method.getModifiers();
        if (modifiers.contains(Modifier.STATIC)) {
            return Optional.empty();
        }

        if (!method.getParameters().isEmpty() || !method.getTypeParameters().isEmpty()) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Method may not have any parameters",
                method
            );
            return Optional.empty();
        }

        return Optional.of(ImmutableMember.builder().owner(root).method(method).build());
    }

    @ValueClass
    interface Spec {
        TypeElement root();

        TypeMirror rootType();

        List<Member> members();
    }

    @ValueClass
    interface Member {
        TypeElement owner();

        ExecutableElement method();

        @Value.Derived
        default String name() {
            return method().getSimpleName().toString();
        }
    }
}
