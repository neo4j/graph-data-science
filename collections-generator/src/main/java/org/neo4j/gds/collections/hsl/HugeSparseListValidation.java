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
package org.neo4j.gds.collections.hsl;

import com.google.auto.common.MoreElements;
import com.google.auto.common.MoreTypes;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.collections.CollectionStep;
import org.neo4j.gds.collections.HugeSparseList;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.Name;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Optional;

import static com.google.auto.common.AnnotationMirrors.getAnnotationValue;

final class HugeSparseListValidation implements CollectionStep.Validation<HugeSparseListValidation.Spec> {

    private final Types typeUtils;
    private final Elements elementUtils;
    private final Messager messager;

    HugeSparseListValidation(Types typeUtils, Elements elementUtils, Messager messager) {
        this.typeUtils = typeUtils;
        this.elementUtils = elementUtils;
        this.messager = messager;
    }

    @Override
    public Optional<Spec> validate(Element element) {
        var annotationMirror = MoreElements.getAnnotationMirror(element, HugeSparseList.class).get();
        var valueType = (TypeMirror) getAnnotationValue(annotationMirror, "valueType").getValue();

        if (!isValidValueType(valueType)) {
            return Optional.empty();
        }

        var forAllConsumerType = (TypeMirror) getAnnotationValue(annotationMirror, "forAllConsumerType").getValue();
        var isArrayType = valueType.getKind() == TypeKind.ARRAY;
        var pageShift = (int) getAnnotationValue(annotationMirror, "pageShift").getValue();
        var drainingIteratorType = elementUtils.getTypeElement("org.neo4j.gds.collections.DrainingIterator").asType();

        var elementValidator = new ElementValidator(
            typeUtils,
            forAllConsumerType,
            drainingIteratorType,
            isArrayType,
            this.messager
        );

        if (!isValid(element, elementValidator, valueType)) {
            return Optional.empty();
        }

        var rootPackage = rootPackage(element);

        var spec = ImmutableSpec.builder()
            .element(element)
            .valueType(valueType)
            .forAllConsumerType(forAllConsumerType)
            .rootPackage(rootPackage)
            .pageShift(pageShift)
            .build();

        return Optional.of(spec);
    }

    private Name rootPackage(Element element) {
        return elementUtils.getPackageOf(element).getQualifiedName();
    }

    private boolean isValid(Element element, ElementValidator validator, TypeMirror annotationValue) {
        return element
            .getEnclosedElements()
            .stream()
            // We do not use `allMatch` in order to run all validations and not stop on the first failing one.
            .map(e -> e.accept(validator, annotationValue))
            .reduce(true, (a, b) -> a && b);
    }

    private boolean isValidValueType(TypeMirror valueType) {
        var isArray = valueType.getKind() == TypeKind.ARRAY;
        if (isArray) {
            var componentType = MoreTypes.asArray(valueType).getComponentType();
            return isValidValueType(componentType);
        }

        var isPrimitive = valueType.getKind().isPrimitive();
        if (!isPrimitive) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "value type must be a primitive type or a primitive array type"
            );
            return false;
        }

        return true;
    }

    @ValueClass
    public interface Spec extends CollectionStep.Spec {
        Element element();

        TypeMirror valueType();

        TypeMirror forAllConsumerType();

        int pageShift();

        @Override
        Name rootPackage();

        default String className() {
            return element().getSimpleName() + "Son";
        }
    }
}
