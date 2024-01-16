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
package org.neo4j.gds.junit;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.platform.commons.util.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Optional;

/**
 * Maps the parameter index to a parameter value.
 * This is a helper for {@link MappedParameterResolver} that allows us
 * to map the parameter index of @BeforeEach or @AfterEach to the parameter
 * value at the same index in the corresponding @ParameterizedTest.
 */
public class MappedParameterContext implements ParameterContext {

    private final int index;
    private final Parameter parameter;
    private final Optional<Object> target;

    MappedParameterContext(int index, Parameter parameter, Optional<Object> target) {
        this.index = index;
        this.parameter = parameter;
        this.target = target;
    }

    @Override
    public boolean isAnnotated(Class<? extends Annotation> annotationType) {
        return AnnotationUtils.isAnnotated(parameter, annotationType);
    }

    @Override
    public <A extends Annotation> Optional<A> findAnnotation(Class<A> annotationType) {
        return AnnotationUtils.findAnnotation(parameter, annotationType);
    }

    @Override
    public <A extends Annotation> List<A> findRepeatableAnnotations(Class<A> annotationType) {
        return AnnotationUtils.findRepeatableAnnotations(parameter, annotationType);
    }

    @Override
    public Parameter getParameter() {
        return this.parameter;
    }

    @Override
    public int getIndex() {
        return this.index;
    }

    @Override
    public Optional<Object> getTarget() {
        return this.target;
    }
}
