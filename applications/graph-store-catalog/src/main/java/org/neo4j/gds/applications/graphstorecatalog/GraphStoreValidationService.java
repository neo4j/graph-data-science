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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GraphStoreValidationService {
    /**
     * @throws java.lang.IllegalArgumentException if at least one key in the list of node properties is not present in the graph store
     */
    public void ensureNodePropertiesExist(GraphStore graphStore, Collection<String> nodeProperties) {
        var invalidProperties = nodeProperties.stream()
            .filter(nodeProperty -> !graphStore.hasNodeProperty(nodeProperty))
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not find property key(s) %s. Defined keys: %s.",
                StringJoining.join(invalidProperties),
                StringJoining.join(graphStore.nodePropertyKeys())
            ));
        }
    }
}
