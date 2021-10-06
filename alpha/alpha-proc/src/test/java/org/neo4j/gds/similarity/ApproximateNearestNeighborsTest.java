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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoTestBase;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.similarity.ApproxNearestNeighborsAlgorithm;
import org.neo4j.gds.impl.similarity.ApproximateNearestNeighborsConfig;
import org.neo4j.gds.impl.similarity.CategoricalInput;
import org.neo4j.gds.impl.similarity.ImmutableApproximateNearestNeighborsConfig;
import org.neo4j.gds.impl.similarity.ImmutableJaccardConfig;
import org.neo4j.gds.impl.similarity.JaccardAlgorithm;
import org.neo4j.gds.impl.similarity.JaccardConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.gds.compat.MapUtil.genericMap;
import static org.neo4j.gds.core.loading.ImportSizing.MIN_PAGE_SIZE;

class ApproximateNearestNeighborsTest extends AlgoTestBase {

    @Test
    void testRunningAnnWithIdGaps() {
        Transaction transaction = db.beginTx();

        for (long i = 0; i < MIN_PAGE_SIZE; i++) {
            transaction.createNode(Label.label("IGNORE"));
        }

        Collection<Long> categories = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        Collection<Map<String, Object>> inputData = new ArrayList<>();
        for (long i = 0; i < categories.size(); i++) {
            Node node = transaction.createNode(Label.label("LOAD"));
            inputData.add(genericMap(
                "item", node.getId(),
                "categories", categories
            ));
        }

        transaction.commit();
        transaction.close();

        JaccardConfig jaccardConfig = ImmutableJaccardConfig.builder().build();
        JaccardAlgorithm jaccardAlgorithm = new JaccardAlgorithm(jaccardConfig, db);

        ApproximateNearestNeighborsConfig annConfig = ImmutableApproximateNearestNeighborsConfig
            .builder()
            .algorithm(ApproximateNearestNeighborsConfig.SimilarityAlgorithm.jaccard)
            .addAllData(inputData)
            .build();

        ApproxNearestNeighborsAlgorithm<CategoricalInput> ann = new ApproxNearestNeighborsAlgorithm<>(
            annConfig,
            jaccardAlgorithm,
            db,
            new TestLog(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        SimilarityAlgorithmResult result = ann.compute();

        assertFalse(result.isEmpty());
        result.stream().forEach(res -> assertEquals(1.0, res.similarity));
    }

}
