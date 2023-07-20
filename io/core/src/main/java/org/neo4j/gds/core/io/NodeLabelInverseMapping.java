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

import org.neo4j.gds.NodeLabel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NodeLabelInverseMapping {
    // index -> label name
    private final HashMap<String, String> map;

    public NodeLabelInverseMapping() {
        map = new HashMap<>();
    }

    public void add(String index, String labelName) {
        map.put(index, labelName);
    }

    public String get(String index) {
        return map.get(index);
    }

    public Set<Map.Entry<String, String>> entrySet() {
        return map.entrySet();
    }
}
