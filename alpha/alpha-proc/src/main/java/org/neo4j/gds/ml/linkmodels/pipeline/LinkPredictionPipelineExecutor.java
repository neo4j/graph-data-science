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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LinkPredictionPipelineExecutor extends PipelineExecutor<LinkFeatureStep, double[]> {

    private final LinkPredictionSplitConfig splitConfig;
    private final String userName;
    private final NamedDatabaseId databaseId;
    private final RelationshipSplitter relationshipSplitter;

    public LinkPredictionPipelineExecutor(
        LinkPredictionPipeline pipeline,
        LinkPredictionTrainConfig config,
        LinkPredictionSplitConfig splitConfig,
        BaseProc caller,
        NamedDatabaseId databaseId,
        String userName,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(
            pipeline,
            config,
            caller,
            GraphStoreCatalog.get(CatalogRequest.of(userName, databaseId.name()), graphName).graphStore(),
            graphName,
            config.concurrency(),
            progressTracker
        );
        this.splitConfig = splitConfig;
        this.userName = userName;
        this.databaseId = databaseId;
        this.relationshipSplitter = new RelationshipSplitter(graphName, splitConfig, caller, progressTracker);
    }

    @Override
    protected Map<DatasetSplits, PipelineExecutor.GraphFilter> splitDataset() {
        return null;
    }

    @Override
    protected HugeObjectArray<double[]> extractFeatures(
        Graph graph, List<LinkFeatureStep> linkFeatureSteps, int concurrency, ProgressTracker progressTracker
    ) {
        return null;
    }

    @Override
    protected void train(HugeObjectArray<double[]> features) {

    }

    public LinkFeatureExtractor linkFeatureExtractor(Graph graph) {
        return LinkFeatureExtractor.of(graph, pipeline.featureSteps());
    }

    public void splitRelationships(
        GraphStore graphStore,
        List<String> relationshipTypes,
        List<String> nodeLabels,
        Optional<Long> randomSeed,
        Optional<String> relationshipWeightProperty
    ) {
        this.relationshipSplitter.splitRelationships(
            graphStore,
            relationshipTypes,
            nodeLabels,
            randomSeed,
            relationshipWeightProperty
        );
    }
}
