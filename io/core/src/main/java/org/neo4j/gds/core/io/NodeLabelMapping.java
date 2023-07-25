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
package org.neo4j.gds.core.io;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeLabelMapping {
    private final HashMap<NodeLabel, String> map;

    NodeLabelMapping(Collection<NodeLabel> labels) {
        MutableInt index = new MutableInt();
        map = new HashMap<>(labels.stream()
            .collect(Collectors.toMap(label -> label, label -> Integer.toString(index.getAndIncrement()))));
    }

    public String get(NodeLabel label) {
        return map.get(label);
    }

    public Set<Map.Entry<NodeLabel, String>> entrySet() {
        return map.entrySet();
    }
}
