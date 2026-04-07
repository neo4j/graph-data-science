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
package org.neo4j.gds.pathfinding.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.validation.AlgorithmGraphStoreRequirements;
import org.neo4j.gds.core.loading.validation.CompoundAlgorithmGraphStoreRequirements;
import org.neo4j.gds.core.loading.validation.OptionalNodePropertyGraphStoreRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.TargetNodesRequirement;
import org.neo4j.gds.maxflow.MaxFlowParameters;

import java.util.Collection;
import java.util.List;

public class FlowAlgorithmRequirements implements AlgorithmGraphStoreRequirements {

    private final SourceNodesRequirement sourceNodes;
    private final TargetNodesRequirement targetNodes;
    private final OptionalNodePropertyGraphStoreRequirement nodeCapacityProperty;

     FlowAlgorithmRequirements(
        SourceNodesRequirement sourceNodes,
        TargetNodesRequirement targetNodes,
        OptionalNodePropertyGraphStoreRequirement nodeCapacityProperty
    ) {
        this.sourceNodes = sourceNodes;
        this.targetNodes = targetNodes;
        this.nodeCapacityProperty = nodeCapacityProperty;
    }


    public static FlowAlgorithmRequirements create(MaxFlowParameters parameters){
        return new FlowAlgorithmRequirements(
            new SourceNodesRequirement(parameters.sourceNodes().inputNodes()),
            new TargetNodesRequirement(parameters.targetNodes().inputNodes()),
            OptionalNodePropertyGraphStoreRequirement.create(parameters.nodeCapacityProperty())
        );
    }


    @Override
    public void validate(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        //TODO: merge the refactoring that replaces the `AlgorithmRequirementsBuilder`
        new CompoundAlgorithmGraphStoreRequirements(List.of(sourceNodes,targetNodes,nodeCapacityProperty))
            .validate(graphStore,selectedLabels,selectedRelationshipTypes);
    }



}
