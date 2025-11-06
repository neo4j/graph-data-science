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
package org.neo4j.gds.community;

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.louvain.LouvainResult;

import java.util.Collection;

public class LouvainMutateStep implements MutateStep<LouvainResult, NodePropertiesWritten> {
    private final StandardCommunityProperties standardCommunityProperties;
    private final boolean includeIntermediateCommunities;
    private final SpecificCommunityMutateStep specificCommunityMutateStep;

    public LouvainMutateStep(
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty,
        String seedProperty,
        boolean isIncremental,
        boolean consecutiveIds,
        boolean includeIntermediateCommunities
    ) {
        this.specificCommunityMutateStep = new SpecificCommunityMutateStep(mutateNodePropertyService,labelsToUpdate,mutateProperty);
        this.standardCommunityProperties = new StandardCommunityProperties(
            isIncremental,
            seedProperty,
            consecutiveIds,
            mutateProperty
        );
        this.includeIntermediateCommunities = includeIntermediateCommunities;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        LouvainResult result
    ) {

        return specificCommunityMutateStep.apply(graph,graphStore,nodePropertyValues(graphStore,result));
    }

    private NodePropertyValues nodePropertyValues(GraphStore graphStore, LouvainResult louvainResult){
        if (includeIntermediateCommunities){
            return CommunityCompanion.createIntermediateCommunitiesNodePropertyValues(
                louvainResult::intermediateCommunities,
                louvainResult.size()
            );
        }
        return standardCommunityProperties.compute(
            graphStore,
            NodePropertyValuesAdapter.adapt(louvainResult.dendrogramManager().getCurrent()));
    }
}
