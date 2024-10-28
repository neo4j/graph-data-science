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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.Optional;
import java.util.stream.Stream;

public final class LocalPipelinesProcedureFacade implements PipelinesProcedureFacade {
    private final PipelineApplications pipelineApplications;

    private final LinkPredictionFacade linkPredictionFacade;
    private final NodeClassificationFacade nodeClassificationFacade;
    private final NodeRegressionFacade nodeRegressionFacade;

    private LocalPipelinesProcedureFacade(
        PipelineApplications pipelineApplications,
        LinkPredictionFacade linkPredictionFacade,
        NodeClassificationFacade nodeClassificationFacade,
        NodeRegressionFacade nodeRegressionFacade
    ) {
        this.pipelineApplications = pipelineApplications;
        this.linkPredictionFacade = linkPredictionFacade;
        this.nodeClassificationFacade = nodeClassificationFacade;
        this.nodeRegressionFacade = nodeRegressionFacade;
    }

    public static PipelinesProcedureFacade create(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        PipelineRepository pipelineRepository,
        CloseableResourceRegistry closeableResourceRegistry,
        DatabaseId databaseId,
        DependencyResolver dependencyResolver,
        Metrics metrics,
        NodeLookup nodeLookup,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        ProcedureReturnColumns procedureReturnColumns,
        RelationshipExporterBuilder relationshipExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationMonitor terminationMonitor,
        TerminationFlag terminationFlag,
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        var pipelineConfigurationParser = new PipelineConfigurationParser(user);

        var pipelineApplications = PipelineApplications.create(
            log,
            graphStoreCatalogService,
            modelCatalog,
            modelRepository,
            pipelineRepository,
            closeableResourceRegistry,
            databaseId,
            dependencyResolver,
            metrics,
            nodeLookup,
            nodePropertyExporterBuilder,
            procedureReturnColumns,
            relationshipExporterBuilder,
            taskRegistryFactory,
            terminationMonitor,
            terminationFlag,
            user,
            userLogRegistryFactory,
            pipelineConfigurationParser,
            progressTrackerCreator,
            algorithmsProcedureFacade,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate
        );

        var linkPredictionFacade = LocalLinkPredictionFacade.create(
            user,
            pipelineConfigurationParser,
            pipelineApplications,
            pipelineRepository
        );

        var nodeClassificationFacade = LocalNodeClassificationFacade.create(
            modelCatalog,
            user,
            pipelineConfigurationParser,
            pipelineApplications,
            pipelineRepository
        );

        var nodeRegressionFacade = LocalNodeRegressionFacade.create(modelCatalog, user, pipelineApplications);

        return new LocalPipelinesProcedureFacade(
            pipelineApplications,
            linkPredictionFacade,
            nodeClassificationFacade,
            nodeRegressionFacade
        );
    }

    @Override
    public Stream<PipelineCatalogResult> drop(
        String pipelineNameAsString,
        boolean failIfMissing
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        if (failIfMissing) {
            var result = pipelineApplications.dropAcceptingFailure(pipelineName);

            return Stream.of(PipelineCatalogResultTransformer.create(result, pipelineName.value));
        }

        var result = pipelineApplications.dropSilencingFailure(pipelineName);

        if (result == null) return Stream.empty();

        var pipelineCatalogResult = PipelineCatalogResultTransformer.create(result, pipelineName.value);

        return Stream.of(pipelineCatalogResult);
    }

    @Override
    public Stream<PipelineExistsResult> exists(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipelineType = pipelineApplications.exists(pipelineName);

        if (pipelineType.isEmpty()) return Stream.of(PipelineExistsResult.empty(pipelineName.value));

        var result = new PipelineExistsResult(pipelineName.value, pipelineType.get(), true);

        return Stream.of(result);
    }

    @Override
    public Stream<PipelineCatalogResult> list(String pipelineNameAsString) {
        if (pipelineNameAsString == null || pipelineNameAsString.equals(NO_VALUE)) {
            var pipelineEntries = pipelineApplications.getAll();

            return pipelineEntries.map(
                entry -> PipelineCatalogResultTransformer.create(
                    entry.pipeline(),
                    entry.pipelineName()
                )
            );
        }

        var pipelineName = PipelineName.parse(pipelineNameAsString);

        Optional<TrainingPipeline<?>> pipeline = pipelineApplications.getSingle(pipelineName);

        if (pipeline.isEmpty()) return Stream.empty();

        var result = PipelineCatalogResultTransformer.create(pipeline.get(), pipelineName.value);

        return Stream.of(result);
    }

    @Override
    public LinkPredictionFacade linkPrediction() {
        return linkPredictionFacade;
    }

    @Override
    public NodeClassificationFacade nodeClassification() {
        return nodeClassificationFacade;
    }

    @Override
    public NodeRegressionFacade nodeRegression() {
        return nodeRegressionFacade;
    }
}
