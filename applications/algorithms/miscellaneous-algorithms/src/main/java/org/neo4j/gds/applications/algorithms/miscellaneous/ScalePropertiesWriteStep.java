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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.algorithms.misc.ScaledPropertiesNodePropertyValues;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ScaleProperties;

class ScalePropertiesWriteStep implements WriteStep<ScalePropertiesResult, NodePropertiesWritten> {
    private final WriteToDatabase writeToDatabase;
    private final ScalePropertiesWriteConfig configuration;

    ScalePropertiesWriteStep(WriteToDatabase writeToDatabase, ScalePropertiesWriteConfig configuration) {
        this.writeToDatabase = writeToDatabase;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        ScalePropertiesResult result,
        JobId jobId
    ) {
        var nodePropertyValues = new ScaledPropertiesNodePropertyValues(graph.nodeCount(), result.scaledProperties());

        return writeToDatabase.perform(
            graph,
            graphStore,
            resultStore,
            configuration,
            configuration,
            ScaleProperties,
            jobId,
            nodePropertyValues
        );
    }
}
