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
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.procedures.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.misc.MiscAlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;
import org.neo4j.gds.procedures.similarity.SimilarityProcedureFacade;

/**
 * I have been postponing this. It is _mainly_ helpful for tests. Just some code structure convenience.
 * I am not, however, putting any convenience in here like a default no-op logger,
 * nor defaulting to @{@link org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService#PASSTHROUGH},
 * because this is after all used in production code.
 */
public class GraphDataScienceProceduresBuilder {
    private final Log log;
    private CatalogFacade catalogFacade;
    private CentralityProcedureFacade centralityProcedureFacade;
    private CommunityProcedureFacade communityProcedureFacade;
    private MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade;
    private NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade;
    private PathFindingProcedureFacade pathFindingProcedureFacade;
    private PipelinesProcedureFacade pipelinesProcedureFacade;
    private SimilarityProcedureFacade similarityProcedureFacade;
    private DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    public GraphDataScienceProceduresBuilder(Log log) {
        this.log = log;
    }

    public GraphDataScienceProceduresBuilder with(CatalogFacade catalogFacade) {
        this.catalogFacade = catalogFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(CentralityProcedureFacade centralityProcedureFacade) {
        this.centralityProcedureFacade = centralityProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(CommunityProcedureFacade communityProcedureFacade) {
        this.communityProcedureFacade = communityProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade) {
        this.miscAlgorithmsProcedureFacade = miscAlgorithmsProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade) {
        this.nodeEmbeddingsProcedureFacade = nodeEmbeddingsProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(PathFindingProcedureFacade pathFindingProcedureFacade) {
        this.pathFindingProcedureFacade = pathFindingProcedureFacade;
        return this;
    }

    /**
     * @deprecated this stops working the moment I need pipelines to use algorithms. At that point: opinionated builder
     */
    @Deprecated
    public GraphDataScienceProceduresBuilder with(PipelinesProcedureFacade pipelinesProcedureFacade) {
        this.pipelinesProcedureFacade = pipelinesProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(SimilarityProcedureFacade similarityProcedureFacade) {
        this.similarityProcedureFacade = similarityProcedureFacade;
        return this;
    }

    public GraphDataScienceProceduresBuilder with(DeprecatedProceduresMetricService deprecatedProceduresMetricService) {
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
        return this;
    }

    public GraphDataScienceProcedures build() {
        var algorithmsProcedureFacade = new AlgorithmsProcedureFacade(pathFindingProcedureFacade);

        return new GraphDataScienceProcedures(
            log,
            algorithmsProcedureFacade,
            catalogFacade,
            centralityProcedureFacade,
            communityProcedureFacade,
            miscAlgorithmsProcedureFacade,
            nodeEmbeddingsProcedureFacade,
            pipelinesProcedureFacade,
            similarityProcedureFacade,
            deprecatedProceduresMetricService
        );
    }
}
