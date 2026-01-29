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

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Optional;
import java.util.function.Function;

public class GenericCentralityWriteStep<R extends CentralityAlgorithmResult> implements WriteStep<R, NodePropertiesWritten> {
    private final WriteNodePropertyService writeNodePropertyService;
    private final Label algorithmLabel;
    private final Function<ResultStore, Optional<ResultStore>> resultStoreResolver;
    private final Concurrency writeConcurrency;
    private final String writeProperty;

    public GenericCentralityWriteStep(
        WriteNodePropertyService writeNodePropertyService,
        Label algorithmLabel, Function<ResultStore, Optional<ResultStore>> resultStoreResolver,
        Concurrency writeConcurrency,
        String writeProperty
    ) {
        this.writeNodePropertyService = writeNodePropertyService;
        this.algorithmLabel = algorithmLabel;
        this.resultStoreResolver = resultStoreResolver;
        this.writeConcurrency = writeConcurrency;
        this.writeProperty = writeProperty;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        R result,
        JobId jobId
    ) {
        return writeNodePropertyService.perform(
            graph,
            graphStore,
            resultStoreResolver.apply(resultStore),
            writeConcurrency,
            algorithmLabel,
            jobId,
            writeProperty,
            result.nodePropertyValues()
        );
    }
}
