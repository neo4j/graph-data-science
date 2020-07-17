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
package org.neo4j.graphalgo.pregel;

import com.google.auto.common.MoreElements;
import com.google.auto.common.MoreTypes;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class PregelValidation {

    private final Messager messager;
    private final Types typeUtils;

    // the interface
    private final TypeMirror pregelComputation;

    PregelValidation(Messager messager, Elements elementUtils, Types typeUtils) {
        this.messager = messager;
        this.typeUtils = typeUtils;
        this.pregelComputation = elementUtils.getTypeElement(PregelComputation.class.getName()).asType();
    }

    boolean validate(Element element) {
        if (element.getKind() != ElementKind.CLASS) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated configuration must be a class.",
                element
            );
            return false;
        }

        var pregelElement = MoreElements.asType(element);

        // TODO: this check needs to bubble up the inheritance tree
        var isPregelComputation = pregelElement
            .getInterfaces()
            .stream()
            .anyMatch(tm -> typeUtils.isSameType(tm, pregelComputation));

        if (!isPregelComputation) {
            messager.printMessage(Diagnostic.Kind.ERROR, formatWithLocale(
                "Class must inherit %s",
                MoreTypes.asTypeElement(pregelComputation).getSimpleName()
            ), pregelElement);
            return false;
        }

        return true;
    }
}
