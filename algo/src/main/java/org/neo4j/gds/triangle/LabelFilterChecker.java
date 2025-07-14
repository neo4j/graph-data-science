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
package org.neo4j.gds.triangle;

import org.neo4j.gds.NodeLabel;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public class LabelFilterChecker {

    private final Optional<NodeLabel> aLabel;
    private final Optional<NodeLabel> bLabel;
    private final Optional<NodeLabel> cLabel;
    private final BiFunction<Long, NodeLabel, Boolean> hasLabel;

    public LabelFilterChecker(List<String> labelFilter, BiFunction<Long, NodeLabel, Boolean> hasLabel) {
        this.hasLabel = hasLabel;
        if (!labelFilter.isEmpty()) {
            this.aLabel = Optional.of(NodeLabel.of(labelFilter.getFirst()));
        } else {
            this.aLabel = Optional.empty();
        }
        if (labelFilter.size() > 1) {
            this.bLabel = Optional.of(NodeLabel.of(labelFilter.get(1)));
        } else {
            this.bLabel = Optional.empty();
        }
        if (labelFilter.size() > 2) {
            this.cLabel = Optional.of(NodeLabel.of(labelFilter.get(2)));
        } else {
            this.cLabel = Optional.empty();
        }
    }

    public boolean check(long nodeId) {
        return cLabel.isEmpty() || bLabel.isEmpty() || aLabel.isEmpty()
            || hasLabel.apply(nodeId, aLabel.get())
            || hasLabel.apply(nodeId, bLabel.get())
            || hasLabel.apply(nodeId, cLabel.get());
    }

    public boolean check(long aNodeId, long bNodeId) {
        // No filters
        return (aLabel.isEmpty() && bLabel.isEmpty() && cLabel.isEmpty())
            // One filter
            || (bLabel.isEmpty() && cLabel.isEmpty() && (hasLabel.apply(aNodeId, aLabel.get()) || hasLabel.apply(bNodeId, aLabel.get())))
            // Two filters
            || (cLabel.isEmpty() && aLabel.isPresent() && bLabel.isPresent()
            && (hasLabel.apply(aNodeId, aLabel.get()) || hasLabel.apply(aNodeId, bLabel.get()) || hasLabel.apply(bNodeId, aLabel.get()) && hasLabel.apply(bNodeId, bLabel.get())))
            // Three filters
            || (aLabel.isPresent() && bLabel.isPresent() && cLabel.isPresent()
            && ((hasLabel.apply(aNodeId, aLabel.get()) && hasLabel.apply(bNodeId, bLabel.get()))
            || (hasLabel.apply(aNodeId, aLabel.get()) && hasLabel.apply(bNodeId, cLabel.get()))
            || (hasLabel.apply(aNodeId, bLabel.get()) && hasLabel.apply(bNodeId, aLabel.get()))
            || (hasLabel.apply(aNodeId, bLabel.get()) && hasLabel.apply(bNodeId, cLabel.get()))
            || (hasLabel.apply(aNodeId, cLabel.get()) && hasLabel.apply(bNodeId, aLabel.get()))
            || (hasLabel.apply(aNodeId, cLabel.get()) && hasLabel.apply(bNodeId, bLabel.get()))));
    }

    public boolean check(long aNodeId, long bNodeId, long cNodeId) {
        // No filters
        return (aLabel.isEmpty() && bLabel.isEmpty() && cLabel.isEmpty())
            // One filter
            || (bLabel.isEmpty() && cLabel.isEmpty() && (hasLabel.apply(aNodeId, aLabel.get()) || hasLabel.apply(bNodeId, aLabel.get()) || hasLabel.apply(cNodeId, aLabel.get())))
            // Two filters
            || (cLabel.isEmpty() && aLabel.isPresent() && bLabel.isPresent()
            && ((hasLabel.apply(aNodeId, bLabel.get()) && (hasLabel.apply(bNodeId, aLabel.get()) || hasLabel.apply(cNodeId, aLabel.get())))
            || (hasLabel.apply(bNodeId, bLabel.get()) && (hasLabel.apply(aNodeId, aLabel.get()) || hasLabel.apply(cNodeId, aLabel.get())))
            || (hasLabel.apply(cNodeId, bLabel.get()) && (hasLabel.apply(aNodeId, aLabel.get()) || hasLabel.apply(bNodeId, aLabel.get())))))
            // Three filters
            || (aLabel.isPresent() && bLabel.isPresent() && cLabel.isPresent())
            && ((hasLabel.apply(aNodeId, aLabel.get()) && hasLabel.apply(bNodeId, bLabel.get()) && hasLabel.apply(cNodeId, cLabel.get()))
            || (hasLabel.apply(aNodeId, aLabel.get()) && hasLabel.apply(cNodeId, aLabel.get()) && hasLabel.apply(bNodeId, cLabel.get()))
            || (hasLabel.apply(bNodeId, aLabel.get()) && hasLabel.apply(aNodeId, bLabel.get()) && hasLabel.apply(cNodeId, cLabel.get()))
            || (hasLabel.apply(bNodeId, aLabel.get()) && hasLabel.apply(cNodeId, bLabel.get()) && hasLabel.apply(aNodeId, cLabel.get()))
            || (hasLabel.apply(cNodeId, aLabel.get()) && hasLabel.apply(aNodeId, bLabel.get()) && hasLabel.apply(bNodeId, cLabel.get()))
            || (hasLabel.apply(cNodeId, aLabel.get()) && hasLabel.apply(bNodeId, bLabel.get()) && hasLabel.apply(aNodeId, cLabel.get())));
    }
}
