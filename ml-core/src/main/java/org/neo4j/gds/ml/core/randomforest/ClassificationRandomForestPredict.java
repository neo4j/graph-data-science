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
package org.neo4j.gds.ml.core.randomforest;

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.ml.core.decisiontree.DecisionTreePredict;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ClassificationRandomForestPredict {

    private final DecisionTreePredict<Integer>[] decisionTrees;
    private final int[] classes;
    private final Map<Integer, Integer> classToIdx;
    private final int concurrency;

    public ClassificationRandomForestPredict(
        DecisionTreePredict<Integer>[] decisionTrees,
        int[] classes,
        Map<Integer, Integer> classToIdx,
        int concurrency
    ) {
        this.decisionTrees = decisionTrees;
        this.classes = classes;
        this.concurrency = concurrency;
        this.classToIdx = classToIdx;
    }

    int predict(final double[] features) {
        final var predictionsPerClass = new AtomicIntegerArray(classes.length);
        var tasks = ParallelUtil.tasks(this.decisionTrees.length, index -> () -> {
            var tree = this.decisionTrees[index];
            var prediction = tree.predict(features);
            predictionsPerClass.incrementAndGet(classToIdx.get(prediction));
        });
        ParallelUtil.runWithConcurrency(this.concurrency, tasks, Pools.DEFAULT);

        int max = -1;
        int maxClassIdx = 0;
        for (int i = 0; i < predictionsPerClass.length(); i++) {
            var numPredictions = predictionsPerClass.get(i);

            if (numPredictions <= max) continue;

            max = numPredictions;
            maxClassIdx = i;
        }

        return classes[maxClassIdx];
    }
}
