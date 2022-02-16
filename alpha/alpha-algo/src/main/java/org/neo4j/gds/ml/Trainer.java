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
package org.neo4j.gds.ml;

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

public interface Trainer {

    Classifier train(Features features, HugeLongArray labels);

    interface Classifier {
        default int numberOfClasses() {
            return classIdMap().size();
        }

        LocalIdMap classIdMap();

        long predict(long id, Features features);

        double[] predictProbabilities(long id, Features features);

        ClassifierData data();

    }

    // placeholder
    interface ClassifierData {}

    interface Features {
        long size();

        double[] get(long id);

        static Features wrap(HugeObjectArray<double[]> features) {
            return new Features() {
                @Override
                public long size() {
                    return features.size();
                }

                @Override
                public double[] get(long id) {
                    return features.get(id);
                }
            };
        }

        static Features wrap(double[] features) {
            return new Features() {
                @Override
                public long size() {
                    return 1;
                }

                @Override
                public double[] get(long id) {
                    assert id == 0;
                    return features;
                }
            };
        }
    }
}
