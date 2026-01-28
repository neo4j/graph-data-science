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
package org.neo4j.gds.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.influenceMaximization.CELFNodeProperties;
import org.neo4j.gds.influenceMaximization.CELFResult;

import java.util.Optional;
import java.util.function.Function;

public class CelfWriteStep implements WriteStep<CELFResult, NodePropertiesWritten> {
    private final WriteNodePropertyService writeNodePropertyService;
    private final Function<ResultStore, Optional<ResultStore>> resultStoreResolver;
    private final Concurrency writeConcurrency;
    private final String writeProperty;

    public CelfWriteStep(WriteNodePropertyService writeNodePropertyService,
        Function<ResultStore, Optional<ResultStore>> resultStoreResolver,
        Concurrency writeConcurrency,
        String writeProperty
    ) {
        this.writeNodePropertyService = writeNodePropertyService;
        this.resultStoreResolver = resultStoreResolver;
        this.writeConcurrency = writeConcurrency;
        this.writeProperty = writeProperty;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        CELFResult result,
        JobId jobId
    ) {
        var nodePropertyValues = new CELFNodeProperties(result.seedSetNodes(), graph.nodeCount());

        return writeNodePropertyService.perform(
            graph,
            graphStore,
            resultStoreResolver.apply(resultStore),
            writeConcurrency,
            AlgorithmLabel.CELF,
            jobId,
            writeProperty,
            nodePropertyValues
        );
    }
}
