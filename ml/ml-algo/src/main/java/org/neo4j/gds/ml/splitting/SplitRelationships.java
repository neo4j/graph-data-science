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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.negativeSampling.NegativeSampler;
import org.neo4j.gds.ml.negativeSampling.RandomNegativeSampler;

import java.util.Optional;

public final class SplitRelationships extends Algorithm<EdgeSplitter.SplitResult> {

    private final Graph graph;
    private final Graph masterGraph;


    private final IdMap rootNodes;

    private final IdMap sourceNodes;

    private final IdMap targetNodes;
    private final SplitRelationshipsParameters parameters;

    private SplitRelationships(
        Graph graph,
        Graph masterGraph,
        IdMap rootNodes,
        IdMap sourceNodes,
        IdMap targetNodes,
        SplitRelationshipsParameters parameters
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.masterGraph = masterGraph;
        this.rootNodes = rootNodes;
        this.sourceNodes = sourceNodes;
        this.targetNodes = targetNodes;
        this.parameters = parameters;
    }

    public static SplitRelationships of(GraphStore graphStore, SplitRelationshipsBaseConfig config) {
        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var sourceLabels = ElementTypeValidator.resolve(graphStore, config.sourceNodeLabels());
        var targetLabels = ElementTypeValidator.resolve(graphStore, config.targetNodeLabels());
        var relationshipTypes = config.internalRelationshipTypes(graphStore);
        var superRelationshipTypes = ElementTypeValidator.resolveTypes(graphStore, config.superRelationshipTypes());

        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, config.relationshipWeightProperty());
        var masterGraph = graphStore.getGraph(nodeLabels, superRelationshipTypes, Optional.empty());

        IdMap sourceNodes = graphStore.getGraph(sourceLabels);
        IdMap targetNodes = graphStore.getGraph(targetLabels);

        return new SplitRelationships(graph,
            masterGraph,
            graphStore.nodes(),
            sourceNodes,
            targetNodes,
            SplitRelationshipConfigTransformer.toParameters(config)
        );
    }

    @Override
    public EdgeSplitter.SplitResult compute() {
        boolean isUndirected = graph.schema().isUndirected();
        var splitter = isUndirected ? new UndirectedEdgeSplitter(parameters.randomSeed(),
            rootNodes,
            sourceNodes,
            targetNodes,
            parameters.holdoutRelationshipType(),
            parameters.remainingRelationshipType(),
            parameters.concurrency()
        ) : new DirectedEdgeSplitter(parameters.randomSeed(),
            rootNodes,
            sourceNodes,
            targetNodes,
            parameters.holdoutRelationshipType(),
            parameters.remainingRelationshipType(),
            parameters.concurrency()
        );

        var splitResult = splitter.splitPositiveExamples(graph,
            parameters.holdoutFraction(),
            parameters.relationshipWeightProperty()
        );

        NegativeSampler negativeSampler = new RandomNegativeSampler(masterGraph,
            (long) (splitResult.selectedRelCount() * parameters.negativeSamplingRatio()),
            //SplitRelationshipsProc does not add negative samples to holdout set
            0,
            sourceNodes,
            targetNodes,
            parameters.randomSeed()
        );

        negativeSampler.produceNegativeSamples(splitResult.selectedRels(), null);

        return splitResult;
    }

}
