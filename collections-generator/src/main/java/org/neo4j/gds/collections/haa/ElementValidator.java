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
package org.neo4j.gds.collections.haa;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleElementVisitor9;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import static org.neo4j.gds.collections.ValidatorUtils.doesNotThrow;
import static org.neo4j.gds.collections.ValidatorUtils.hasNoParameters;
import static org.neo4j.gds.collections.ValidatorUtils.hasParameterCount;
import static org.neo4j.gds.collections.ValidatorUtils.hasSingleLongParameter;
import static org.neo4j.gds.collections.ValidatorUtils.hasTypeAtIndex;
import static org.neo4j.gds.collections.ValidatorUtils.hasTypeKindAtIndex;
import static org.neo4j.gds.collections.ValidatorUtils.isAbstract;
import static org.neo4j.gds.collections.ValidatorUtils.isDefault;
import static org.neo4j.gds.collections.ValidatorUtils.isNotGeneric;
import static org.neo4j.gds.collections.ValidatorUtils.mustReturn;

final class ElementValidator extends SimpleElementVisitor9<Boolean, TypeMirror> {

    private final TypeMirror rootType;
    private final TypeMirror unaryOperatorType;
    private final Messager messager;
    private final Types typeUtils;

    ElementValidator(
        TypeMirror rootType,
        TypeMirror unaryOperatorType,
        Messager messager,
        Types typeUtils
    ) {
        super(false);
        this.rootType = rootType;
        this.unaryOperatorType = unaryOperatorType;
        this.messager = messager;
        this.typeUtils = typeUtils;
    }

    @Override
    protected Boolean defaultAction(Element e, TypeMirror aClass) {
        messager.printMessage(Diagnostic.Kind.ERROR, "Unexpected enclosed element", e);
        return false;
    }

    @Override
    public Boolean visitType(TypeElement e, TypeMirror elementType) {
        // no inner type expected
        return true;
    }

    @Override
    public Boolean visitExecutable(ExecutableElement e, TypeMirror elementType) {
        if (e.getModifiers().contains(Modifier.STATIC)) {
            // ignore static methods for validation
            return true;
        }

        switch (e.getSimpleName().toString()) {
            case "defaultValue":
                return validateDefaultValueMethod(e, elementType);
            case "get":
                return validateGetMethod(e, elementType);
            case "getAndAdd":
                return validateGetAndAddMethod(e, elementType);
            case "getAndReplace":
                return validateGetAndReplaceMethod(e, elementType);
            case "set":
                return validateSetMethod(e, elementType);
            case "compareAndSet":
                return validateCompareAndSetMethod(e, elementType);
            case "compareAndExchange":
                return validateCompareAndExchangeMethod(e, elementType);
            case "update":
                return validateUpdateMethod(e);
            case "size":
                return validateSizeMethod(e);
            case "sizeOf":
                return validateSizeOfMethod(e);
            case "setAll":
                return validateSetAllMethod(e, elementType);
            case "release":
                return validateReleaseMethod(e);
            case "copyTo":
                return validateCopyToMethod(e);
            default:
                messager.printMessage(Diagnostic.Kind.ERROR, "unexpected method", e);
        }

        return false;
    }

    private boolean validateDefaultValueMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, elementType.getKind(), messager)
               && hasNoParameters(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isDefault(e, messager);
    }

    private boolean validateGetMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, elementType.getKind(), messager)
               && hasSingleLongParameter(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateGetAndAddMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, elementType.getKind(), messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateGetAndReplaceMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, elementType.getKind(), messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }


    private boolean validateSetMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, TypeKind.VOID, messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateCompareAndSetMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, TypeKind.BOOLEAN, messager)
               && hasParameterCount(e, 3, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && hasTypeKindAtIndex(e, 2, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateCompareAndExchangeMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, elementType.getKind(), messager)
               && hasParameterCount(e, 3, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && hasTypeKindAtIndex(e, 2, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateUpdateMethod(ExecutableElement e) {
        return mustReturn(e, TypeKind.VOID, messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeAtIndex(typeUtils, e, 1, unaryOperatorType, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateSizeMethod(ExecutableElement e) {
        return mustReturn(e, TypeKind.LONG, messager)
               && hasNoParameters(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateSizeOfMethod(ExecutableElement e) {
        return mustReturn(e, TypeKind.LONG, messager)
               && hasNoParameters(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateSetAllMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, TypeKind.VOID, messager)
               && hasParameterCount(e, 1, messager)
               && hasTypeKindAtIndex(e, 0, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateReleaseMethod(ExecutableElement e) {
        return mustReturn(e, TypeKind.LONG, messager)
               && hasNoParameters(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateCopyToMethod(ExecutableElement e) {
        return mustReturn(e, TypeKind.VOID, messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeAtIndex(typeUtils, e, 0, rootType, messager)
               && hasTypeKindAtIndex(e, 1, TypeKind.LONG, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }
}
