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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenWriteConfig;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Leiden;

class LeidenWriteStep implements WriteStep<LeidenResult, Pair<NodePropertiesWritten, NodePropertyValues>> {
    private final WriteNodePropertyService writeNodePropertyService;
    private final LeidenWriteConfig configuration;

    LeidenWriteStep(WriteNodePropertyService writeNodePropertyService, LeidenWriteConfig configuration) {
        this.writeNodePropertyService = writeNodePropertyService;
        this.configuration = configuration;
    }

    @Override
    public Pair<NodePropertiesWritten, NodePropertyValues> execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        LeidenResult result,
        JobId jobId
    ) {
        var nodePropertyValues = constructNodePropertyValues(graphStore, result);

        var nodePropertiesWritten = writeNodePropertyService.perform(
            graph,
            graphStore,
            resultStore,
            configuration,
            Leiden,
            jobId,
            nodePropertyValues
        );

        return Pair.of(nodePropertiesWritten, nodePropertyValues);
    }

    private NodePropertyValues constructNodePropertyValues(GraphStore graphStore, LeidenResult result) {
        if (configuration.includeIntermediateCommunities())
            return CommunityCompanion.createIntermediateCommunitiesNodePropertyValues(
                result::intermediateCommunities,
                result.communities().size()
            );

        return CommunityCompanion.nodePropertyValues(
            configuration.isIncremental(),
            configuration.writeProperty(),
            configuration.seedProperty(),
            configuration.consecutiveIds(),
            NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent()),
            configuration.minCommunitySize(),
            configuration.concurrency(),
            () -> graphStore.nodeProperty(configuration.seedProperty())
        );
    }
}
