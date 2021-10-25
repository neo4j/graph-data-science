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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;

public abstract class PipelineExecutor<FEATURE_STEP extends FeatureStep, FEATURE_TYPE> {

    public enum DatasetSplits {
        TRAIN,
        TEST,
        TEST_COMPLEMENT,
        FEATURE_INPUT
    }

    protected final PipelineBuilder<FEATURE_STEP> pipeline;
    protected final AlgoBaseConfig config;
    protected final BaseProc caller;
    protected final GraphStore graphStore;
    protected final String graphName;
    protected final int concurrency;
    protected final ProgressTracker progressTracker;

    public PipelineExecutor(
        PipelineBuilder<FEATURE_STEP> pipeline,
        AlgoBaseConfig config,
        BaseProc caller,
        GraphStore graphStore,
        String graphName,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        this.pipeline = pipeline;
        this.config = config;
        this.caller = caller;
        this.graphStore = graphStore;
        this.graphName = graphName;
        this.concurrency = concurrency;
        this.progressTracker = progressTracker;
    }

    protected abstract Map<DatasetSplits, GraphFilter> splitDataset();

    protected abstract HugeObjectArray<FEATURE_TYPE> extractFeatures(Graph graph, List<FEATURE_STEP> featureSteps, int concurrency, ProgressTracker progressTracker);

    protected abstract void train(HugeObjectArray<FEATURE_TYPE> features);

    public void execute() {
        var datasets = splitDataset();

        var featureInputGraphFilter = datasets.get(DatasetSplits.FEATURE_INPUT);
        executeNodePropertySteps(featureInputGraphFilter.nodeLabels(), featureInputGraphFilter.relationshipTypes());

        var trainGraphFilter = datasets.get(DatasetSplits.TRAIN);
        var features = computeFeatures(
            trainGraphFilter.nodeLabels(),
            trainGraphFilter.relationshipTypes(),
            this.concurrency
        );

        train(features);

        cleanUpGraphStore(datasets);
    }

    private HugeObjectArray<FEATURE_TYPE> computeFeatures(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipType,
        int concurrency
    ) {
        var graph = graphStore.getGraph(nodeLabels, relationshipType, Optional.empty());

        pipeline.validate(graph);

        return extractFeatures(graph, pipeline.featureSteps(), concurrency, progressTracker);
    }

    private void executeNodePropertySteps(
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

    private void removeNodeProperties(GraphStore graphstore, Collection<NodeLabel> nodeLabels) {
        pipeline.nodePropertySteps().forEach(step -> {
            var intermediateProperty = step.config().get(MUTATE_PROPERTY_KEY);
            if (intermediateProperty instanceof String) {
                nodeLabels.forEach(label -> graphstore.removeNodeProperty(label, ((String) intermediateProperty)));
            }
        });
    }

    private void cleanUpGraphStore(Map<DatasetSplits, GraphFilter> datasets) {
        datasets.values()
            .stream()
            .flatMap(graphFilter -> graphFilter.relationshipTypes().stream()).forEach(graphStore::deleteRelationships);

        removeNodeProperties(graphStore, config.nodeLabelIdentifiers(graphStore));
    }

    @ValueClass
    public interface GraphFilter {
        List<NodeLabel> nodeLabels();
        List<RelationshipType> relationshipTypes();
    }
}
