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
import java.util.SplittableRandom;

public final class FeatureBagger {

    private final SplittableRandom random;
    private final int totalNumberOfFeatures;
    private final int[] featureBag;

    public static FeatureBagger of(SplittableRandom random, int totalNumberOfFeatures, double featureBaggingRatio) {
        var featureBagger = new FeatureBagger(random, totalNumberOfFeatures, featureBaggingRatio);

        if (featureBagger.featureBag.length == totalNumberOfFeatures) {
            // cache everything is sampled
            for (int i = 0; i < featureBagger.featureBag.length; i++) {
                featureBagger.featureBag[i] = i;
            }
        }

        return featureBagger;
    }

    private FeatureBagger(SplittableRandom random, int totalNumberOfFeatures, double featureBaggingRatio) {
        assert Double.compare(featureBaggingRatio, 0) != 0: "Invalid featureBaggingRatio";

        this.random = random;
        this.totalNumberOfFeatures = totalNumberOfFeatures;
        this.featureBag = new int[(int) Math.ceil(featureBaggingRatio * totalNumberOfFeatures)];
    }

    int[] sample() {
        if (totalNumberOfFeatures == featureBag.length) {
            // everything is sampled
            return featureBag;
        }

        // TODO: Can we replace with UniformSamplerByRange?
        var tmpAvailableIndices = new Integer[totalNumberOfFeatures];
        Arrays.setAll(tmpAvailableIndices, i -> i);
        final var availableIndices = new LinkedList<>(Arrays.asList(tmpAvailableIndices));

        for (int i = 0; i < featureBag.length; i++) {
            int j = random.nextInt(availableIndices.size());
            featureBag[i] = availableIndices.get(j);
            availableIndices.remove(j);
        }

        return featureBag;
    }
}
