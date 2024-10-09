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
package org.neo4j.gds.pricesteiner;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.function.LongPredicate;

public final class TreeProducer {

    private TreeProducer() {}

    static TreeStructure createTree(GrowthResult growthResult,long nodeCount, IdMap idMap, ProgressTracker progressTracker){

        progressTracker.beginSubTask("Tree Creation");

        var treeEdges = growthResult.treeEdges();
        var numberOfTreeEdges = growthResult.numberOfTreeEdges();
        LongPredicate activePredicate = growthResult.activeOriginalNodes()::get;
        var edgeParts  = growthResult.edgeParts();
        var edgeCosts = growthResult.edgeCosts();

        var degree = HugeLongArray.newArray(nodeCount);

        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(RelationshipType.of("_IGNORED_"))
            .orientation(Orientation.UNDIRECTED)
            .addPropertyConfig(GraphFactory.PropertyConfig.builder()
                .propertyKey("property")
                .aggregation(Aggregation.SUM)
                .build())
            .build();

        for (long i =0; i<numberOfTreeEdges;++i){
            var edgeId = treeEdges.get(i);
            var  u = Math.abs(edgeParts.get(2*edgeId));
            var  v = Math.abs(edgeParts.get(2*edgeId+1));
            if (activePredicate.test(u) && activePredicate.test(v)){
                    degree.addTo(u,1);
                    degree.addTo(v,1);
                    relationshipsBuilder.addFromInternal(u,v,edgeCosts.get(edgeId));
                    progressTracker.logProgress();
            }
        }
        var singleTypeRelationships= relationshipsBuilder.build();
        var tree = GraphFactory.create(idMap, singleTypeRelationships);


        progressTracker.endSubTask("Tree Creation");
        return new TreeStructure(tree,degree,  idMap.nodeCount());


    }

}
record TreeStructure(Graph tree, HugeLongArray degrees, long originalNodeCount){}

