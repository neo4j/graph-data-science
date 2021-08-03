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

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class NodeLabel extends ElementIdentifier {

    public static final NodeLabel ALL_NODES = NodeLabel.of("__ALL__");

    public NodeLabel(String name) {
        super(name);
    }

    @Override
    public ElementIdentifier projectAll() {
        return ALL_NODES;
    }

    public static NodeLabel of(@NotNull String name) {
        return new NodeLabel(name);
    }

    public static Collection<NodeLabel> listOf(@NotNull String... nodeLabels) {
        return Arrays.stream(nodeLabels).map(NodeLabel::of).collect(Collectors.toList());
    }
}
