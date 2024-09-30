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
package org.neo4j.gds.procedures;

import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.catalog.GraphCatalogProcedureFacade;
import org.neo4j.gds.procedures.modelcatalog.ModelCatalogProcedureFacade;
import org.neo4j.gds.procedures.operations.OperationsProcedureFacade;
import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;

/**
 * I have been postponing this. It is _mainly_ helpful for tests. Just some code structure convenience.
 * I am not, however, putting any convenience in here like a default no-op logger,
 * nor defaulting to @{@link org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService#DISABLED},
 * because this is after all used in production code.
 */
public class GraphDataScienceProceduresBuilder {
    private final Log log;
    private AlgorithmsProcedureFacade algorithmsProcedureFacade;
    private GraphCatalogProcedureFacade graphCatalogProcedureFacade;
    private ModelCatalogProcedureFacade modelCatalogProcedureFacade;
    private OperationsProcedureFacade operationsProcedureFacade;
    private PipelinesProcedureFacade pipelinesProcedureFacade;
    private DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    public GraphDataScienceProceduresBuilder(Log log) {
        this.log = log;
    }

    public GraphDataScienceProceduresBuilder with(AlgorithmsProcedureFacade algorithmsProcedureFacade) {
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(GraphCatalogProcedureFacade graphCatalogProcedureFacade) {
        this.graphCatalogProcedureFacade = graphCatalogProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(ModelCatalogProcedureFacade modelCatalogProcedureFacade) {
        this.modelCatalogProcedureFacade = modelCatalogProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(OperationsProcedureFacade operationsProcedureFacade) {
        this.operationsProcedureFacade = operationsProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(PipelinesProcedureFacade pipelinesProcedureFacade) {
        this.pipelinesProcedureFacade = pipelinesProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(DeprecatedProceduresMetricService deprecatedProceduresMetricService) {
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
        return this;
    }

    public GraphDataScienceProcedures build() {
        return new LocalGraphDataScienceProcedures(
            log,
            algorithmsProcedureFacade,
            graphCatalogProcedureFacade,
            modelCatalogProcedureFacade,
            operationsProcedureFacade,
            pipelinesProcedureFacade,
            deprecatedProceduresMetricService
        );
    }
}
