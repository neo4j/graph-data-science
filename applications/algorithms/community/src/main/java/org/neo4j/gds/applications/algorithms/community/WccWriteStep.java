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
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.wcc.WccWriteConfig;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.WCC;

class WccWriteStep implements WriteStep<DisjointSetStruct, Pair<NodePropertiesWritten, NodePropertyValues>> {
    private final WriteToDatabase writeToDatabase;
    private final WccWriteConfig configuration;

    WccWriteStep(WriteToDatabase writeToDatabase, WccWriteConfig configuration) {
        this.writeToDatabase = writeToDatabase;
        this.configuration = configuration;
    }

    @Override
    public Pair<NodePropertiesWritten, NodePropertyValues> execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        DisjointSetStruct result,
        JobId jobId
    ) {
        var nodePropertyValues = CommunityCompanion.nodePropertyValues(
            configuration.isIncremental(),
            configuration.seedProperty(),
            configuration.writeProperty(),
            configuration.consecutiveIds(),
            result.asNodeProperties(),
            configuration.minCommunitySize(),
            configuration.concurrency(),
            () -> graphStore.nodeProperty(configuration.seedProperty())
        );

        var nodePropertiesWritten = writeToDatabase.perform(
            graph,
            graphStore,
            resultStore,
            configuration,
            configuration,
            WCC,
            jobId,
            nodePropertyValues
        );

        return Pair.of(nodePropertiesWritten, nodePropertyValues);
    }
}
