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
package org.neo4j.graphalgo.core.utils.mem;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A tree shaped description of an object that has resources residing in memory.
 */
public interface MemoryTree {

    /**
     * @return a textual description for this component.
     */
    String description();

    /**
     * @return The resident memory of this component.
     */
    MemoryRange memoryUsage();

    /**
     * @return nested resources of this component.
     */
    default Collection<MemoryTree> components() {
        return Collections.emptyList();
    }

    default Map<String, Object> renderMap() {
        Map<String, Object> root = new HashMap<>();
        root.put("name", description());
        root.put("memoryUsage", memoryUsage().toString());
        List<Map<String, Object>> components = components()
                .stream()
                .map(MemoryTree::renderMap)
                .collect(Collectors.toList());
        if (!components.isEmpty()) {
            root.put("components", components);
        }
        return root;
    }

    /**
     * Renders the memory requirements into a human readable representation.
     */
    default String render() {
        StringBuilder sb = new StringBuilder();
        render(sb, this, 0);
        return sb.toString();
    }

    static void render(
            final StringBuilder sb,
            final MemoryTree estimation,
            final int depth) {
        for (int i = 1; i < depth; i++) {
            sb.append("    ");
        }

        if (depth > 0) {
            sb.append("|-- ");
        }

        sb.append(estimation.description());
        sb.append(": ");
        sb.append(estimation.memoryUsage());
        sb.append(System.lineSeparator());

        for (final MemoryTree component : estimation.components()) {
            render(sb, component, depth + 1);
        }
    }

    static MemoryTree empty() {
        return NULL_TREE;
    }

    MemoryTree NULL_TREE = new MemoryTree() {
        @Override
        public String description() {
            return "";
        }

        @Override
        public MemoryRange memoryUsage() {
            return MemoryRange.empty();
        }
    };
}
