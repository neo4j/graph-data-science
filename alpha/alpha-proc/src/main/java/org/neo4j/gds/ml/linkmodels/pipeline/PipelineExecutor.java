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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor.extractFeatures;

public class PipelineExecutor {

    private final LinkPredictionPipelineBuilder pipeline;
    private final String userName;
    private final NamedDatabaseId databaseId;
    private final BaseProc caller;
    private final String graphName;
    private final RelationshipSplitter relationshipSplitter;
    private final ProgressTracker progressTracker;

    public PipelineExecutor(
        LinkPredictionPipelineBuilder pipeline,
        BaseProc caller,
        NamedDatabaseId databaseId,
        String userName,
        String graphName,
        ProgressTracker progressTracker
    ) {
        this.pipeline = pipeline;
        this.caller = caller;
        this.userName = userName;
        this.databaseId = databaseId;
        this.graphName = graphName;
        this.relationshipSplitter = new RelationshipSplitter(graphName, pipeline, caller, progressTracker);
        this.progressTracker = progressTracker;
    }

    public HugeObjectArray<double[]> computeFeatures(
        Collection<NodeLabel> nodeLabels,
        RelationshipType relationshipType,
        int concurrency
    ) {
        var graph = GraphStoreCatalog.get(CatalogRequest.of(userName, databaseId.name()), graphName)
            .graphStore()
            .getGraph(nodeLabels, List.of(relationshipType), Optional.empty());

        pipeline.validate(graph);

        return extractFeatures(graph, pipeline.featureSteps(), concurrency, progressTracker);
    }

    public LinkFeatureExtractor linkFeatureExtractor(Graph graph) {
        return LinkFeatureExtractor.of(graph, pipeline.featureSteps());
    }

    public void executeNodePropertySteps(Collection<NodeLabel> nodeLabels, RelationshipType relationshipType) {
        executeNodePropertySteps(nodeLabels, List.of(relationshipType));
    }

    public void executeNodePropertySteps(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes
    ) {
        progressTracker.beginSubTask("execute node property steps");
        for (NodePropertyStep step : pipeline.nodePropertySteps()) {
            progressTracker.beginSubTask();
            step.execute(caller, graphName, nodeLabels, relationshipTypes);
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask("execute node property steps");
    }

    public void removeNodeProperties(GraphStore graphstore, Collection<NodeLabel> nodeLabels) {
        pipeline.nodePropertySteps().forEach(step -> {
            var intermediateProperty = step.config().get(MUTATE_PROPERTY_KEY);
            if (intermediateProperty instanceof String) {
                nodeLabels.forEach(label -> graphstore.removeNodeProperty(label, ((String) intermediateProperty)));
            }
        });
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
