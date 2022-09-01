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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;

import java.util.Collection;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LabelNodeFilterSpec implements NodeFilterSpec {

    private final String labelString;

    LabelNodeFilterSpec(String labelString) {
        this.labelString = labelString;
    }

    @Override
    public NodeFilter toNodeFilter(IdMap idMap) {
        return LabelNodeFilter.create(labelString, idMap);
    }

    @Override
    public String render() {
        return "NodeFilter[label=" + labelString + "]";
    }

    @Override
    public void validate(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        String nodeFilterType
    ) throws IllegalArgumentException {

        var nodeLabelIsMissing = !graphStore.nodeLabels().contains(NodeLabel.of(labelString));

        if (nodeLabelIsMissing) {
            var errorMessage = formatWithLocale(
                "Invalid configuration value '%s', the node label `%s` is missing from the graph.",
                nodeFilterType,
                labelString
            );
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
