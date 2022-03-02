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
package org.neo4j.gds.ml.nodemodels;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.Features;
import org.neo4j.gds.ml.Trainer;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.logisticregression.TestFeatures;
import org.openjdk.jol.util.Multiset;

import java.util.List;

import static org.neo4j.gds.ml.nodemodels.NodeClassificationPredictConsumerTest.idMapOf;
import static org.neo4j.gds.ml.nodemodels.metrics.AllClassMetric.F1_WEIGHTED;

class ClassificationMetricComputerTest {

    @Test
    void shouldComputeMetrics() {
        var multiSet = new Multiset<Long>();
        multiSet.add(0L, 2);
        multiSet.add(1L, 1);
        multiSet.add(3L, 1);
        var idMap = idMapOf(1, 0, 3, 0);
        var features = new TestFeatures(new double[][]{new double[]{2.4}, new double[]{2.9}, new double[]{-3.1}, new double[]{0.0}});
        var targets = HugeLongArray.of(1, 0, 3, 0);

        var classificationMetricComputer = new ClassificationMetricComputer(
            List.of(F1_WEIGHTED),
            multiSet,
            features,
            targets,
            1,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var classifier = new Trainer.Classifier() {
            @Override
            public LocalIdMap classIdMap() {
                return idMap;
            }

            @Override
            public double[] predictProbabilities(long id, Features features) {
                if (id == 0) {
                    return new double[]{0.2, 0.8};
                } else {
                    return new double[]{0.7, 0.3};
                }
            }

            @Override
            public Trainer.ClassifierData data() {
                return null;
            }
        };
//        classificationMetricComputer.computeMetrics(evaluationSet, )
    }
}
