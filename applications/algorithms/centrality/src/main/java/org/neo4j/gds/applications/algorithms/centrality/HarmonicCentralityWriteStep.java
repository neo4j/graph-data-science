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
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicResult;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.HarmonicCentrality;

class HarmonicCentralityWriteStep implements MutateOrWriteStep<HarmonicResult, NodePropertiesWritten> {
    private final WriteToDatabase writeToDatabase;
    private final HarmonicCentralityWriteConfig configuration;

    HarmonicCentralityWriteStep(WriteToDatabase writeToDatabase, HarmonicCentralityWriteConfig configuration) {
        this.writeToDatabase = writeToDatabase;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        HarmonicResult result,
        JobId jobId
    ) {
        return writeToDatabase.perform(
            graph,
            graphStore,
            resultStore,
            configuration,
            configuration,
            HarmonicCentrality,
            result,
            jobId
        );
    }
}
