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

import com.google.auto.common.MoreElements;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.collections.HugeSparseArray;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.Name;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import java.util.Optional;

import static com.google.auto.common.AnnotationMirrors.getAnnotationValue;

final class HugeSparseArrayValidation {

    private final Elements elementUtils;
    private final Messager messager;

    HugeSparseArrayValidation(Elements elementUtils, Messager messager) {
        this.elementUtils = elementUtils;
        this.messager = messager;
    }

    Optional<Spec> validate(Element element) {
        var annotationMirror = MoreElements.getAnnotationMirror(element, HugeSparseArray.class).get();
        var annotationValue = (TypeMirror) getAnnotationValue(annotationMirror, "type").getValue();

        if (!hasValidEnclosingElements(element, annotationValue)) {
            return Optional.empty();
        }

        var rootPackage = rootPackage(element);

        return Optional.of(Spec.of(element, rootPackage));
    }

    private Name rootPackage(Element element) {
        return elementUtils.getPackageOf(element).getQualifiedName();
    }

    private boolean hasValidEnclosingElements(Element element, TypeMirror annotationValue) {
        var elementValidator = new ElementValidator(element.asType(), this.messager);
        return element
            .getEnclosedElements()
            .stream()
            // We do not use `allMatch` in order to run all validations and not stop on the first failing one.
            .map(e -> e.accept(elementValidator, annotationValue))
            .reduce(true, (a, b) -> a && b);
    }

    @ValueClass
    public interface Spec {
        Element element();

        Name rootPackage();

        default String className() {
            return element().getSimpleName() + "Son";
        }

        static Spec of(Element element, Name rootPackage) {
            return ImmutableSpec.builder().element(element).rootPackage(rootPackage).build();
        }
    }
}
