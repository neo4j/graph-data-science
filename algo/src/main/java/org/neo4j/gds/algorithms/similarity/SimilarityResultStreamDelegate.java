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
package org.neo4j.gds.algorithms.similarity;

import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * "Delegate" because really we should mint a microtype and place this behaviour in it
 */
public class SimilarityResultStreamDelegate {

    public void consumeStream(
        Concurrency concurrency,
        Stream<SimilarityResult> similarityResultStream,
        TerminationFlag terminationFlag,
        RelationshipWithPropertyConsumer consumer
    ){
        Consumer<SimilarityResult> consumableAction = (similarityResult ->
            consumer.accept(
                similarityResult.sourceNodeId(),
                similarityResult.targetNodeId(),
                similarityResult.similarity
            )
        );
        if (concurrency.sequential()){
            similarityResultStream.forEach(consumableAction);
        }else {
            ParallelUtil.parallelStreamConsume(
                similarityResultStream,
                concurrency,
                terminationFlag,
                similarityStream -> similarityStream.forEach(consumableAction)
                );

        }
    }

}
