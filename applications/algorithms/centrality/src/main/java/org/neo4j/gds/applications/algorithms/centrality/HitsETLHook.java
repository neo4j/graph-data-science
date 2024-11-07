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
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.indexInverse.InverseRelationships;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfigImpl;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfigTransformer;
import org.neo4j.gds.indexInverse.InverseRelationshipsProgressTaskCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Collectors;

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

        var relationshipTypesWithoutIndex = relationshipTypes
            .stream()
            .filter(relType -> !graphStore.inverseIndexedRelationshipTypes().contains(relType))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (relationshipTypesWithoutIndex.isEmpty()) {
            return;
        }


        var inverseConfig = InverseRelationshipsConfigImpl
            .builder()
            .concurrency(configuration.concurrency().value())
            .relationshipTypes(relationshipTypesWithoutIndex)
            .build();

        var parameters = InverseRelationshipsConfigTransformer.toParameters(inverseConfig);

        var task = InverseRelationshipsProgressTaskCreator.progressTask(graphStore.nodeCount(),relationshipTypes);
        var progressTracker =  progressTrackerCreator.createProgressTracker(inverseConfig,task);

        var inverseRelationships=new InverseRelationships(graphStore,parameters,progressTracker, DefaultPool.INSTANCE,terminationFlag);

        inverseRelationships
            .compute()
            .forEach((relationshipType, inverseIndex) -> graphStore.addInverseIndex(
                relationshipType,
                inverseIndex.topology(),
                inverseIndex.properties()
            ));

    }

}
