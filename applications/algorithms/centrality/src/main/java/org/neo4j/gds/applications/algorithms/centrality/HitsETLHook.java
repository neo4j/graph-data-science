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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.PostLoadETLHook;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.indexInverse.InverseRelationships;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfigImpl;
import org.neo4j.gds.indexInverse.InverseRelationshipsParamsTransformer;
import org.neo4j.gds.indexInverse.InverseRelationshipsTask;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collection;
import java.util.List;

class HitsETLHook implements PostLoadETLHook {
    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;
    private final HitsConfig configuration;

    HitsETLHook(
        HitsConfig configuration,
        ProgressTrackerCreator progressTrackerCreator,
        TerminationFlag terminationFlag
    ) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.configuration = configuration;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public void onGraphStoreLoaded(GraphStore graphStore) {
        var relationshipTypes = configuration.internalRelationshipTypes(graphStore);

        var relationshipTypesWithoutIndex = relationshipsWithoutIndices(graphStore, relationshipTypes);
        if (relationshipTypesWithoutIndex.isEmpty()) {
            return;
        }

        var inverseRelationships = createInverseRelationshipsAlgorithm(graphStore,relationshipTypesWithoutIndex);

        inverseRelationships
            .compute()
            .forEach((relationshipType, inverseIndex) -> addInverseIndex(relationshipType,inverseIndex,graphStore));

    }

    List<String> relationshipsWithoutIndices(GraphStore graphStore, Collection<RelationshipType> relTypes){
        return  relTypes
            .stream()
            .filter(relType -> !graphStore.inverseIndexedRelationshipTypes().contains(relType))
            .map(RelationshipType::name)
            .toList();
    }

    InverseRelationships createInverseRelationshipsAlgorithm(GraphStore graphStore, List<String> relationshipTypesWithoutIndex){
        var inverseConfig = InverseRelationshipsConfigImpl
            .builder()
            .concurrency(configuration.concurrency().value())
            .relationshipTypes(relationshipTypesWithoutIndex)
            .build();

        var parameters = InverseRelationshipsParamsTransformer.toParameters(graphStore, inverseConfig);

        var task = InverseRelationshipsTask.progressTask(graphStore.nodeCount(),parameters);
        var progressTracker =  progressTrackerCreator.createProgressTracker(
            task,
            inverseConfig.jobId(),
            inverseConfig.concurrency(),
            inverseConfig.logProgress()
        );

        return new InverseRelationships(
            graphStore,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );
    }

    void addInverseIndex(RelationshipType relationshipType, SingleTypeRelationships inverseIndex, GraphStore graphStore){
        graphStore.addInverseIndex(
            relationshipType,
            inverseIndex.topology(),
            inverseIndex.properties()
        );
    }

}
