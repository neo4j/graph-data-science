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
package org.neo4j.gds.similarity.knn;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.config.SingleThreadedRandomSeedConfig;

import java.util.List;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface KnnBaseConfig extends AlgoBaseConfig, IterationsConfig, SingleThreadedRandomSeedConfig {

    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.knn.KnnNodePropertySpecParser#parse")
    @Configuration.ToMapValue("org.neo4j.gds.similarity.knn.KnnNodePropertySpecParser#render")
    List<KnnNodePropertySpec> nodeProperties();

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int topK() {
        return 10;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1, minInclusive = false)
    default double sampleRate() {
        return 0.5;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double perturbationRate() {
        return 0.0;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double deltaThreshold() {
        return 0.001;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double similarityCutoff() {
        return 0;
    }

    @Configuration.IntegerRange(min = 1)
    @Override
    @Value.Default
    default int maxIterations() {
        return 100;
    }

    @Value.Default
    @Configuration.Ignore
    default int minBatchSize() {
        return 1_000;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 0)
    default int randomJoins() {
        return 10;
    }

    @Configuration.Ignore
    default int sampledK(long nodeCount) {
        // (int) is safe because value is at most `topK`, which is an int
        // This could be violated if a sampleRate outside of [0,1] is used
        // which is only possible from our tests
        return Math.max(0, (int) Math.min((long) Math.ceil(this.sampleRate() * this.topK()), nodeCount - 1));
    }

    @Configuration.Ignore
    default int boundedK(long nodeCount) {
        // (int) is safe because value is at most `topK`, which is an int
        return Math.max(0, (int) Math.min(this.topK(), nodeCount - 1));
    }

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.similarity.knn.KnnSampler.SamplerType#parse")
    @Configuration.ToMapValue("org.neo4j.gds.similarity.knn.KnnSampler.SamplerType#toString")
    default KnnSampler.SamplerType initialSampler() {
        return KnnSampler.SamplerType.UNIFORM;
    }
}
