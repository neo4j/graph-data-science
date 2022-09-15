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
package org.neo4j.gds.similarity.filteredknn;

import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.similarity.filtering.NodeFilterSpec;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;

import java.util.Collection;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface FilteredKnnBaseConfig extends KnnBaseConfig {

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory#create")
    @Configuration.ToMapValue("org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory#render")
    default NodeFilterSpec sourceNodeFilter() {
        return NodeFilterSpec.noOp;
    }

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory#create")
    @Configuration.ToMapValue("org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory#render")
    default NodeFilterSpec targetNodeFilter() {
        return NodeFilterSpec.noOp;
    }

    @Value.Default
    default boolean seedTargetNodes() {
        return false;
    }

    @Configuration.GraphStoreValidationCheck
    default void validateSourceNodeFilter(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        sourceNodeFilter().validate(graphStore, selectedLabels, "sourceNodeFilter");
    }

    @Configuration.GraphStoreValidationCheck
    default void validateTargetNodeFilter(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        targetNodeFilter().validate(graphStore, selectedLabels, "targetNodeFilter");
    }

}
