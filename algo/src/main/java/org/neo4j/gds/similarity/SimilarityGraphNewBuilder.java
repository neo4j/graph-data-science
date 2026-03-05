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
package org.neo4j.gds.similarity;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.algorithms.similarity.SimilaritySummaryBuilder;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.similarity.nodesim.TopKGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public final class SimilarityGraphNewBuilder {

    private SimilarityGraphNewBuilder() {}

    static TopKSimilarityGraph build(
        TopKGraph topKGraph,
        boolean shouldComputeDistribution
    ){
        return new TopKSimilarityGraph(
            topKGraph,
            shouldComputeDistribution
        );
    }

    public static HugeSimilarityGraph build(
        boolean shouldComputeDistribution,
        Stream<SimilarityResult> stream,
        IdMap idMap,
        Concurrency concurrency,
        ExecutorService executorService,
        TerminationFlag terminationFlag
    ){

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap.rootIdMap())
            .relationshipType(RelationshipType.of("REL"))
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of("property"))
            .concurrency(concurrency)
            .executorService(executorService)
            .build();

        var similaritySummaryBuilder = SimilaritySummaryBuilder.of(concurrency,shouldComputeDistribution);
        ParallelUtil.parallelStreamConsume(
            stream,
            concurrency,
            terminationFlag,
            similarityStream -> similarityStream.forEach(similarityResult -> {
                relationshipsBuilder.addFromInternal(
                idMap.toRootNodeId(similarityResult.sourceNodeId()),
                idMap.toRootNodeId(similarityResult.targetNodeId()),
                similarityResult.similarity);

                similaritySummaryBuilder.accept(
                    similarityResult.sourceNodeId(),
                    similarityResult.targetNodeId(),
                    similarityResult.similarity
                );
                }
            )
        );

        var similarityGraph= GraphFactory.create(
            idMap.rootIdMap(),
            relationshipsBuilder.build()
        );
        return new HugeSimilarityGraph(
            similarityGraph,
            similaritySummaryBuilder.similaritySummary()
        );

    }
}
