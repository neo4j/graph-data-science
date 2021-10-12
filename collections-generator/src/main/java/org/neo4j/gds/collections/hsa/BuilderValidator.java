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
package org.neo4j.gds.collections.hsa;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleElementVisitor9;
import javax.tools.Diagnostic;

import static org.neo4j.gds.collections.hsa.ValidatorUtils.doesNotThrow;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasNoParameters;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasParameterCount;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasTypeKindAtIndex;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.isAbstract;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.isNotGeneric;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.mustReturn;

class BuilderValidator extends SimpleElementVisitor9<Boolean, TypeMirror> {

    private final TypeMirror rootType;
    private final Messager messager;

    BuilderValidator(TypeMirror rootType, Messager messager) {
        this.rootType = rootType;
        this.messager = messager;
    }

    @Override
    protected Boolean defaultAction(Element e, TypeMirror aClass) {
        messager.printMessage(Diagnostic.Kind.ERROR, "Unexpected enclosed element", e);
        return super.defaultAction(e, aClass);
    }

    @Override
    public Boolean visitExecutable(ExecutableElement e, TypeMirror elementType) {
        switch (e.getSimpleName().toString()) {
            case "set":
                return validateSetMethod(e, elementType);
            case "setIfAbsent":
                return validateSetIfAbsentMethod(e, elementType);
            case "addTo":
                return validateAddToMethod(e, elementType);
            case "build":
                return validateBuildMethod(e);
            default:
                messager.printMessage(Diagnostic.Kind.ERROR, "unexpected method", e);
        }

        return false;
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

    private boolean validateSetIfAbsentMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, TypeKind.BOOLEAN, messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateAddToMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, TypeKind.VOID, messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, TypeKind.LONG, messager)
               && hasTypeKindAtIndex(e, 1, elementType.getKind(), messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateBuildMethod(ExecutableElement e) {
        return mustReturn(e, rootType, messager)
               && hasNoParameters(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }
}
