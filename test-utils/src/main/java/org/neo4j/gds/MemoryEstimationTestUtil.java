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
package org.neo4j.gds;

import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import java.util.List;
import java.util.Optional;

public class MemoryEstimationTestUtil {
    public static MemoryTree subTree(MemoryTree tree, String component) {
        var components = tree.components();
        Optional<MemoryTree> maybeSubtree = components.stream().filter(c -> c.description().equals(component)).findFirst();
        return maybeSubtree.orElseThrow(() -> new RuntimeException("There is no component in the memory tree with name " + component));
    }

    public static MemoryTree subTree(MemoryTree tree, List<String> query) {
        var t = tree;
        for (String q : query) {
            t = subTree(t, q);
        }
        return t;
    }
}
