package org.neo4j.graphalgo.core.utils.mem;

import org.neo4j.graphalgo.core.GraphDimensions;

/**
 * A calculation of an object that has resources residing in memory.
 */
@FunctionalInterface
public interface MemoryResident {

    /**
     * @return the number of bytes that this object occupies in memory.
     */
    MemoryRange estimateMemoryUsage(
            GraphDimensions dimensions,
            int concurrency
    );
}
