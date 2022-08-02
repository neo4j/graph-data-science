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
package org.neo4j.gds.config;

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ElementTypeValidator {

    private ElementTypeValidator() {}

    public static Collection<NodeLabel> resolve(GraphStore graphStore, Collection<String> labelFilterNames) {
        return labelFilterNames.contains(ElementProjection.PROJECT_ALL)
            ? graphStore.nodeLabels()
            : labelFilterNames.stream().map(NodeLabel::of).collect(Collectors.toSet());
    }

    public static Collection<RelationshipType> resolveTypes(GraphStore graphStore, Collection<String> relFilterNames) {
        return relFilterNames.contains(ElementProjection.PROJECT_ALL)
            ? graphStore.relationshipTypes()
            : relFilterNames.stream().map(RelationshipType::of).collect(Collectors.toSet());
    }

    public static void validate(GraphStore graphStore, Collection<NodeLabel> labelFilter, String filterName) {
        Set<NodeLabel> availableLabels = graphStore.nodeLabels();

        var invalidLabels = labelFilter
            .stream()
            .filter(label -> !availableLabels.contains(label))
            .map(NodeLabel::name)
            .collect(Collectors.toList());

        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not find %s of %s. Available labels are %s.",
                filterName,
                StringJoining.join(invalidLabels.stream()),
                StringJoining.join(availableLabels.stream().map(NodeLabel::name))
            ));
        }
    }

    public static void validateTypes(GraphStore graphStore, Collection<RelationshipType> relFilter, String filterName) {
        var availableTypes = graphStore.relationshipTypes();

        var invalidLabels = relFilter
            .stream()
            .filter(label -> !availableTypes.contains(label))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not find %s of %s. Available relationship types are %s.",
                filterName,
                StringJoining.join(invalidLabels.stream()),
                StringJoining.join(availableTypes.stream().map(RelationshipType::name))
            ));
        }
    }

    public static Collection<NodeLabel> resolveAndValidate(GraphStore graphStore, Collection<String> labelFilterNames, String filterName) {
        Collection<NodeLabel> nodeLabels = resolve(graphStore, labelFilterNames);
        validate(graphStore, nodeLabels, filterName);

        return nodeLabels;
    }

    public static Collection<RelationshipType> resolveAndValidateTypes(GraphStore graphStore, Collection<String> relFilterNames, String filterName) {
        Collection<RelationshipType> relFilter = resolveTypes(graphStore, relFilterNames);

        validateTypes(graphStore, relFilter, filterName);

        return relFilter;
    }

}
