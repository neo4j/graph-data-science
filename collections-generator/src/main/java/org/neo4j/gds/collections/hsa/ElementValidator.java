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
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleElementVisitor9;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.List;

import static org.neo4j.gds.collections.hsa.ValidatorUtils.doesNotThrow;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasNoParameters;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasParameterCount;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasSingleLongParameter;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasTypeAtIndex;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.hasTypeKindAtIndex;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.isAbstract;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.isNotGeneric;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.isStatic;
import static org.neo4j.gds.collections.hsa.ValidatorUtils.mustReturn;

final class ElementValidator extends SimpleElementVisitor9<Boolean, TypeMirror> {

    private final Types typeUtils;
    private final Messager messager;
    private final BuilderValidator validator;
    private final TypeMirror longConsumerType;

    private TypeElement builderType;

    ElementValidator(
        Types typeUtils,
        TypeMirror rootType,
        TypeMirror longConsumerType,
        boolean isArrayType,
        Messager messager
    ) {
        super(false);
        this.typeUtils = typeUtils;
        this.longConsumerType = longConsumerType;
        this.messager = messager;
        this.validator = new BuilderValidator(rootType, isArrayType, messager);
    }

    TypeElement builderType() {
        return this.builderType;
    }

    @Override
    protected Boolean defaultAction(Element e, TypeMirror aClass) {
        messager.printMessage(Diagnostic.Kind.ERROR, "Unexpected enclosed element", e);
        return super.defaultAction(e, aClass);
    }

    @Override
    public Boolean visitType(TypeElement e, TypeMirror elementType) {
        e.getEnclosedElements().forEach(el -> el.accept(validator, elementType));
        this.builderType = e;
        return true;
    }

    @Override
    public Boolean visitExecutable(ExecutableElement e, TypeMirror elementType) {
        switch (e.getSimpleName().toString()) {
            case "capacity":
                return validateCapacityMethod(e);
            case "get":
                return validateGetMethod(e, elementType);
            case "contains":
                return validateContainsMethod(e);
            case "builder":
                switch (e.getParameters().size()) {
                    case 2:
                        return validateGrowingBuilderMethod(e, elementType);
                    case 3:
                        return validateGrowingBuilderWithInitialCapacityMethod(e, elementType);
                    default:
                        messager.printMessage(
                            Diagnostic.Kind.ERROR,
                            "method has wrong number of parameters, expected one of " + List.of(2, 3),
                            e
                        );
                }
            default:
                messager.printMessage(Diagnostic.Kind.ERROR, "unexpected method", e);
        }

        return false;
    }

    private boolean validateCapacityMethod(ExecutableElement e) {
        return hasNoParameters(e, messager)
               && mustReturn(e, TypeKind.LONG, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateGetMethod(ExecutableElement e, TypeMirror elementType) {
        return mustReturn(e, elementType.getKind(), messager)
               && hasSingleLongParameter(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateContainsMethod(ExecutableElement e) {
        return mustReturn(e, TypeKind.BOOLEAN, messager)
               && hasSingleLongParameter(e, messager)
               && doesNotThrow(e, messager)
               && isNotGeneric(e, messager)
               && isAbstract(e, messager);
    }

    private boolean validateGrowingBuilderMethod(ExecutableElement e, TypeMirror elementType) {
        return doesNotThrow(e, messager)
               && isStatic(e, messager)
               && hasParameterCount(e, 2, messager)
               && hasTypeKindAtIndex(e, 0, elementType.getKind(), messager)
               && hasTypeAtIndex(typeUtils, e, 1, longConsumerType, messager);
    }

    private boolean validateGrowingBuilderWithInitialCapacityMethod(ExecutableElement e, TypeMirror elementType) {
        return doesNotThrow(e, messager)
               && isStatic(e, messager)
               && hasParameterCount(e, 3, messager)
               && hasTypeKindAtIndex(e, 0, elementType.getKind(), messager)
               && hasTypeAtIndex(typeUtils, e, 1, longConsumerType, messager)
               && hasTypeKindAtIndex(e, 2, TypeKind.LONG, messager);
    }
}
