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

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.common.MoreElements;
import com.google.auto.common.MoreTypes;
import com.google.common.collect.ImmutableSetMultimap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.GdsCallable;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

public class ProcedureCollector implements BasicAnnotationProcessor.Step {

    static final String GDS_CALLABLE = GdsCallable.class.getCanonicalName();

    private final Elements elements;
    private final Messager messager;
    private final List<TypeElement> validTypes;

    ProcedureCollector(Elements elements, Messager messager, List<TypeElement> validTypes) {
        this.elements = elements;
        this.messager = messager;
        this.validTypes = validTypes;
    }

    @Override
    public Set<String> annotations() {
        return Set.of(GDS_CALLABLE);
    }

    @Override
    public Set<? extends Element> process(ImmutableSetMultimap<String, Element> elementsByAnnotation) {
        var invalidElements = new HashSet<Element>();
        var elements = elementsByAnnotation.get(GDS_CALLABLE);
        if (elements != null) {
            var algoSpec = this.elements.getTypeElement(AlgorithmSpec.class.getCanonicalName());
            for (var element : elements) {
                if (isInPackage(element)) {
                    var annotation = element.getAnnotationMirrors()
                        .stream()
                        .filter(a -> MoreElements
                            .asType(a.getAnnotationType().asElement())
                            .getQualifiedName()
                            .contentEquals(GDS_CALLABLE)
                        )
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                            "Annotation must be present, otherwise we not not have been called.")
                        );

                    if (isAlgoSpec(element.asType(), algoSpec)) {
                        validTypes.add(MoreElements.asType(element));
                    } else {
                        invalidElements.add(element);

                        messager.printMessage(
                            Diagnostic.Kind.ERROR,
                            String.format(
                                Locale.ENGLISH,
                                "The type annotation with @%s must implement %s",
                                GdsCallable.class.getSimpleName(),
                                AlgorithmSpec.class.getSimpleName()
                            ),
                            element,
                            annotation
                        );
                    }
                }
            }
        }

        return invalidElements;
    }

    private boolean isInPackage(Element element) {
        var thePackage = MoreElements.getPackage(element);
        var packageName = thePackage.getQualifiedName().toString();
        return packageName.startsWith("org.neo4j.gds.");
    }

    private boolean isAlgoSpec(TypeMirror startType, TypeElement algoSpecElement) {
        return Stream
            .iterate(startType, t -> t.getKind() != TypeKind.NONE, t -> MoreTypes.asTypeElement(t).getSuperclass())
            .flatMap(t -> MoreTypes.asTypeElement(t).getInterfaces().stream())
            .anyMatch(i -> MoreTypes.asTypeElement(i).equals(algoSpecElement));
    }
}
