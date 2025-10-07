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
package org.neo4j.gds.community.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyGraphStoreValidation;

import java.util.Collection;
import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TriangleCountGraphStoreValidation extends GraphStoreValidation {

    private final UndirectedOnlyGraphStoreValidation undirectedOnlyGraphStoreValidation;
    private final List<String> labelFilter;

    public static TriangleCountGraphStoreValidation create(List<String> labelFilter){
        return  new TriangleCountGraphStoreValidation(new UndirectedOnlyGraphStoreValidation("Triangle Counting"),labelFilter);
    }
    private TriangleCountGraphStoreValidation(
        UndirectedOnlyGraphStoreValidation undirectedOnlyGraphStoreValidation,
        List<String> labelFilter
    ) {
        this.undirectedOnlyGraphStoreValidation = undirectedOnlyGraphStoreValidation;
        this.labelFilter = labelFilter;
    }


    @Override
    protected void validateAlgorithmRequirements(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
            undirectedOnlyGraphStoreValidation.validateAlgorithmRequirements(graphStore,selectedLabels,selectedRelationshipTypes);
            validateLabelsExist(selectedLabels);
    }


    void validateLabelsExist(
        Collection<NodeLabel> nodeLabels
    ) {
        for (String givenLabelString : labelFilter) {
            var givenLabel = NodeLabel.of(givenLabelString);
            if (!nodeLabels.contains(givenLabel)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "TriangleCount requires the provided 'labelFilter' node label '%s' to be present in the graph.",
                    givenLabel.name()
                ));
            }
        }
    }
}
