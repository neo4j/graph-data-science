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
package org.neo4j.gds.collections;

import com.google.auto.common.MoreTypes;

import javax.annotation.processing.Messager;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.List;

public final class ValidatorUtils {

    public static boolean mustReturn(ExecutableElement e, TypeKind returnKind, Messager messager) {
        if (e.getReturnType().getKind() != returnKind) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method must return " + returnKind.name(), e);
            return false;
        }
        return true;
    }

    public static boolean mustReturn(ExecutableElement e, TypeMirror returnType, Messager messager) {
        if (!MoreTypes.equivalence().equivalent(e.getReturnType(), returnType)) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method must return " + returnType, e);
            return false;
        }
        return true;
    }

    public static boolean doesNotThrow(ExecutableElement e, Messager messager) {
        if (e.getThrownTypes().isEmpty()) {
            return true;
        }
        messager.printMessage(Diagnostic.Kind.ERROR, "Method is not allowed to throw any exceptions", e);
        return false;
    }

    public static boolean isNotGeneric(ExecutableElement e, Messager messager) {
        if (e.getTypeParameters().isEmpty()) {
            return true;
        }
        messager.printMessage(Diagnostic.Kind.ERROR, "Method is not allowed to have generic types", e);
        return false;
    }

    public static boolean isAbstract(ExecutableElement e, Messager messager) {
        if (!e.isDefault() && e.getModifiers().containsAll(List.of(Modifier.ABSTRACT, Modifier.PUBLIC))) {
            return true;
        }

        messager.printMessage(Diagnostic.Kind.ERROR, "Method must be public abstract", e);
        return false;
    }

    public static boolean isDefault(ExecutableElement e, Messager messager) {
        if (e.isDefault()) {
            return true;
        }

        messager.printMessage(Diagnostic.Kind.ERROR, "Method must be default", e);
        return false;
    }

    public static boolean isStatic(ExecutableElement e, Messager messager) {
        if (!e.isDefault() && e.getModifiers().containsAll(List.of(Modifier.STATIC, Modifier.PUBLIC))) {
            return true;
        }

        messager.printMessage(Diagnostic.Kind.ERROR, "Method must be public public static", e);
        return false;
    }

    public static boolean hasNoParameters(ExecutableElement e, Messager messager) {
        return hasParameterCount(e, 0, messager);
    }

    public static boolean hasParameterCount(ExecutableElement e, int expectedCount, Messager messager) {
        if (e.getParameters().size() != expectedCount) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "method has wrong number of parameters, expected " + expectedCount,
                e
            );
            return false;
        }
        return true;
    }

    public static boolean hasTypeKindAtIndex(ExecutableElement e, int index, TypeKind typeKind, Messager messager) {
        var parameters = e.getParameters();

        if (parameters.size() <= index) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method has too few parameters", e);
            return false;
        }

        if (parameters.get(index).asType().getKind() != typeKind) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method must have a parameter of type " + typeKind.name(), e);
            return false;
        }

        return true;
    }

    public static boolean hasTypeAtIndex(
        Types typeUtils,
        ExecutableElement e,
        int index,
        TypeMirror typeMirror,
        Messager messager
    ) {
        var parameters = e.getParameters();

        if (parameters.size() <= index) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method has too few parameters", e);
            return false;
        }


        if (!typeUtils.isSameType(parameters.get(index).asType(), typeMirror)) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "method must have a parameter of type " + typeMirror,
                e
            );
            return false;
        }

        return true;
    }

    public static boolean hasSingleLongParameter(ExecutableElement e, Messager messager) {
        var parameters = e.getParameters();

        if (parameters.size() != 1) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method must have exactly one parameter", e);
            return false;
        }

        if (parameters.get(0).asType().getKind() != TypeKind.LONG) {
            messager.printMessage(Diagnostic.Kind.ERROR, "method must have a parameter of type long", e);
            return false;
        }

        return true;
    }


    private ValidatorUtils() {}
}
