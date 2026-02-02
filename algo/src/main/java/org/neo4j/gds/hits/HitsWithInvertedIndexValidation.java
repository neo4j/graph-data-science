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
package org.neo4j.gds.hits;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.indexInverse.InverseRelationships;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

public class HitsWithInvertedIndexValidation extends Algorithm<PregelResult> {
    private final InverseRelationshipsParameters inverseRelationshipsParameters;
    private final GraphStore graphStore;
    private final Collection<NodeLabel> nodeLabels;
    private final Collection<RelationshipType> relationshipTypesFilter;
    private final Function<Graph, Hits> hitsFunction;

    public HitsWithInvertedIndexValidation(
        ProgressTracker progressTracker,
        InverseRelationshipsParameters inverseRelationshipsParameters,
        GraphStore graphStore,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypesFilter,
        Function<Graph, Hits> hitsFunction
    ) {
        super(progressTracker);
        this.inverseRelationshipsParameters = inverseRelationshipsParameters;
        this.graphStore = graphStore;
        this.nodeLabels = nodeLabels;
        this.relationshipTypesFilter = relationshipTypesFilter;
        this.hitsFunction = hitsFunction;
    }

    @Override
    public PregelResult compute() {
        progressTracker.beginSubTask();
        invertedIndex();
        var graphWithInvertedIndex = getGraph();

        var internalAlgorithm = hitsFunction.apply(graphWithInvertedIndex);
        var result = internalAlgorithm.compute();
        progressTracker.endSubTask();
        return result;
    }

    private Graph getGraph(){
        return graphStore.getGraph(
            nodeLabels,
            relationshipTypesFilter,
            Optional.empty()
        );
    }

    private void invertedIndex(){

        var inverseRelationships = new InverseRelationships(
            graphStore,
            inverseRelationshipsParameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        ).compute();
        inverseRelationships
            .forEach((relationshipType, inverseIndex) ->
                addInverseIndex(relationshipType, inverseIndex, graphStore));


    }

    private void addInverseIndex(
        RelationshipType relationshipType,
        SingleTypeRelationships inverseIndex,
        GraphStore graphStore
    ){
        graphStore.addInverseIndex(
            relationshipType,
            inverseIndex.topology(),
            inverseIndex.properties()
        );
    }


}
