package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;

public abstract class ConfiguredAlgorithm<Self extends ConfiguredAlgorithm<Self, Conf>, Conf> extends Algorithm<Self> {

    public MemoryEstimation memoryEstimation(Conf conf) {
        return this.memoryEstimation();
    }
}
