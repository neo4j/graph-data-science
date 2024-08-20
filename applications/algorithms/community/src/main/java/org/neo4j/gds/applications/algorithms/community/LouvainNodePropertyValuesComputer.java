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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;

public class LouvainNodePropertyValuesComputer {
    public NodePropertyValues compute(
        GraphStore graphStore,
        LouvainBaseConfig configuration,
        String resultProperty,
        LouvainResult result
    ) {
        if (configuration.includeIntermediateCommunities())
            return CommunityCompanion.createIntermediateCommunitiesNodePropertyValues(
                result::intermediateCommunities,
                result.size()
            );

        return CommunityCompanion.nodePropertyValues(
            configuration.isIncremental(),
            resultProperty,
            configuration.seedProperty(),
            configuration.consecutiveIds(),
            NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent()),
            () -> graphStore.nodeProperty(configuration.seedProperty())
        );
    }
}
