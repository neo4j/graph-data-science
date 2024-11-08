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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.hits.HitsConfig;

import java.util.Map;

class HitsWriteStep implements WriteStep<PregelResult, NodePropertiesWritten> {
    private final WriteToDatabase writeToDatabase;
    private final HitsConfig configuration;
    private final Label label;

    HitsWriteStep(
        WriteToDatabase writeToDatabase,
        HitsConfig configuration,
        Label label
    ) {
        this.writeToDatabase = writeToDatabase;
        this.configuration = configuration;
        this.label = label;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        PregelResult result,
        JobId jobId
    ) {
        return writeToDatabase.perform(
            graph,
            graphStore,
            resultStore,
            configuration,
            label,
            jobId,
            nodeProperties(result,configuration)
        );
    }

    Map<String,NodePropertyValues> nodeProperties(PregelResult pregelResult, HitsConfig config){
        var authValues = NodePropertyValuesAdapter.adapt(pregelResult.nodeValues().doubleProperties(config.authProperty()));
        var hubValues = NodePropertyValuesAdapter.adapt(pregelResult.nodeValues().doubleProperties(config.authProperty()));
        var authProperty = config.authProperty().concat(config.writeProperty());
        var hubProperty = config.hubProperty().concat(config.writeProperty());
        return Map.of(authProperty,authValues,hubProperty,hubValues);
    }
}
