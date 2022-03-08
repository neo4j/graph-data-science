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
package org.neo4j.gds.decisiontree;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;

public class FeatureBagger {

    private final Random random;
    private final int totalNumberOfFeatures;
    private final int[] featureBag;

    public FeatureBagger(Random random, int totalNumberOfFeatures, double featureBaggingRatio) {
        assert featureBaggingRatio != 0: "Invalid featureBaggingRatio";

        this.random = random;
        this.totalNumberOfFeatures = totalNumberOfFeatures;
        this.featureBag = new int[(int) Math.ceil(featureBaggingRatio * totalNumberOfFeatures)];

        if (Double.compare(featureBaggingRatio, 1.0D) == 0) {
            // cache everything is sampled
            for (int i = 0; i < featureBag.length; i++) {
                featureBag[i] = i;
            }
        }
    }

    int[] sample() {
        var tmpAvailableIndices = new Integer[totalNumberOfFeatures];
        Arrays.setAll(tmpAvailableIndices, i -> i);
        final var availableIndices = new LinkedList<>(Arrays.asList(tmpAvailableIndices));

        if (totalNumberOfFeatures == featureBag.length) {
            // everything is sampled
            return featureBag;
        }

        for (int i = 0; i < featureBag.length; i++) {
            int j = random.nextInt(availableIndices.size());
            featureBag[i] = (availableIndices.get(j));
            availableIndices.remove(j);
        }

        return featureBag;
    }
}
