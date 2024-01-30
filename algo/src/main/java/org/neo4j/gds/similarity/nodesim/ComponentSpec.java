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
package org.neo4j.gds.similarity.nodesim;

import org.jetbrains.annotations.NotNull;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Specification for how to treat components when computing NodeSimilarity.
 * Components can be used or not used.
 * If components are used, they can be computed or provided.
 */
public final class ComponentSpec {
    /**
     * Valid user input is {@code java.lang.Boolean} or {@code java.lang.String}.
     *
     * False means no components are used.
     * True means components are used, computed internally.
     * String is a property, representing pre-computed components.
     */
    public static ComponentSpec parse(Object userInput) {
        if (userInput instanceof ComponentSpec) { // the config default calls in with a pre-parsed spec. it's annoying.
            return (ComponentSpec) userInput;
        }
        if (userInput instanceof Boolean) {
            return new ComponentSpec((Boolean) userInput, null);
        }
        if (userInput == null) {
            throw new IllegalArgumentException("Invalid component spec: cannot parse null as node property");
        }
        if (userInput instanceof String) {
            return parse((String) userInput);
        }
        throw new IllegalArgumentException(formatWithLocale("Invalid component spec: cannot parse type %s with value %s", userInput.getClass().getSimpleName(), userInput.toString()));
    }

    private static ComponentSpec parse(@NotNull String userInput) {
        if (userInput.isBlank()) {
            throw new IllegalArgumentException("Invalid component spec: expected a valid node property");
        }
        return new ComponentSpec(Boolean.TRUE, userInput);
    }

    public static String render(ComponentSpec spec) {
        return spec.componentProperty == null ? spec.useComponents.toString() : spec.componentProperty;
    }

    static final ComponentSpec NO = new ComponentSpec(false, null);

    private final Boolean useComponents;
    private final String componentProperty;

    private ComponentSpec(Boolean useComponents, String componentProperty) {
        this.useComponents = useComponents;
        this.componentProperty = componentProperty;
    }

    public boolean computeComponents() {
        return useComponents && null == componentProperty;
    }

    public boolean useComponents() {
        return useComponents;
    }

    public boolean usePreComputedComponents() {
        return useComponents && null != componentProperty;
    }

    public String componentProperty() {
        return componentProperty;
    }
}
