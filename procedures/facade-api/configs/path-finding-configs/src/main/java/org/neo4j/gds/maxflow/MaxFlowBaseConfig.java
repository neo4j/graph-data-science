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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.MapInputNodes;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConfigNodesValidations;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SourceNodesWithPropertiesConfig;
import org.neo4j.gds.config.TargetNodesWithPropertiesConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

@Configuration
public interface MaxFlowBaseConfig extends AlgoBaseConfig, RelationshipWeightConfig, SourceNodesWithPropertiesConfig,
    TargetNodesWithPropertiesConfig {

    @Configuration.DoubleRange(min = 0.0, minInclusive = true)
    @Configuration.Ignore
    default double freq() {return 0.5;}

    @Configuration.Ignore
    default boolean useGapRelabelling() {return false;}

    @Override
    @Configuration.Key("capacityProperty")
    Optional<String> relationshipWeightProperty();

    Optional<String> nodeCapacityProperty();

    @Configuration.Check()
    default void assertNoDuplicateInputNodes() {
        var sourceSet = new HashSet<>();
        for (var node : sourceNodes().inputNodes()) {
            if (sourceSet.contains(node)) {
                throw new IllegalArgumentException("Source nodes must be unique.");
            } else {
                sourceSet.add(node);
            }
        }
        var targetSet = new HashSet<>();
        for (var node : targetNodes().inputNodes()) {
            if (targetSet.contains(node)) {
                throw new IllegalArgumentException("Target nodes must be unique.");
            } else if (sourceSet.contains(node)) {
                throw new IllegalArgumentException("Source and target nodes must be disjoint.");
            } else {
                targetSet.add(node);
            }
        }
    }

    @Configuration.Check()
    default void assertNodeValuesArePositive() {
        if (sourceNodes() instanceof MapInputNodes) {
            if (((MapInputNodes) sourceNodes()).map().values().stream().anyMatch(value -> value < 0.0)) {
                throw new IllegalArgumentException("Source node values must be positive, but found a negative value.");
            }
        }
        if (targetNodes() instanceof MapInputNodes) {
            if (((MapInputNodes) targetNodes()).map().values().stream().anyMatch(value -> value < 0.0)) {
                throw new IllegalArgumentException("Target node values must be positive, but found a negative value.");
            }
        }
    }

    @Configuration.Check()
    default void assertSourcesAndTargetsExist() {
        if (sourceNodes().size() == 0) {
            throw new IllegalArgumentException("Source nodes cannot be empty.");
        }
        if (targetNodes().size() == 0) {
            throw new IllegalArgumentException("Target nodes cannot be empty.");
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateNodeCapacityProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {

        if (nodeCapacityProperty().isPresent()) {
            ConfigNodesValidations.validateNodePropertyExists(
                graphStore,
                selectedLabels,
                "Node capacity property",
                nodeCapacityProperty().get()
            );
        }
    }

    @Configuration.Ignore
    default MaxFlowParameters toMaxFlowParameters() {
        return new MaxFlowParameters(
            sourceNodes(),
            targetNodes(),
            concurrency(),
            freq(),
            useGapRelabelling(),
            nodeCapacityProperty()
        );
    }
}
