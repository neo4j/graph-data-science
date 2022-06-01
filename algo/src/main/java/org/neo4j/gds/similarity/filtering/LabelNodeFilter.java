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
package org.neo4j.gds.similarity.filtering;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.IdMap;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LabelNodeFilter implements NodeFilter {

    public static LabelNodeFilter create(String labelString, IdMap idMap) {
        NodeLabel label = null;
        for (var existingLabel : idMap.availableNodeLabels()) {
            if (existingLabel.name.equalsIgnoreCase(labelString)) {
                label = existingLabel;
            }
        }
        if (null == label) {
            throw new IllegalArgumentException(formatWithLocale(
                "The label `%s` does not exist in the graph",
                labelString
            ));
        }
        return new LabelNodeFilter(label, idMap);
    }

    private final NodeLabel label;
    private final IdMap idMap;

    private LabelNodeFilter(NodeLabel label, IdMap idMap) {
        this.label = label;
        this.idMap = idMap;
    }

    @Override
    public boolean test(long nodeId) {
        return idMap.hasLabel(nodeId, label);
    }
}
