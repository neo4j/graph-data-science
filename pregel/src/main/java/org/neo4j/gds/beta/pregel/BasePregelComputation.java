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
package org.neo4j.gds.beta.pregel;

import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.beta.pregel.context.MasterComputeContext;

import java.util.Optional;

public interface BasePregelComputation<C extends PregelConfig> {
    /**
     * The schema describes the node property layout.
     * A node property can be composed of multiple primitive
     * values, such as double or long, as well as arrays of
     * those. Each part of that composite schema is named
     * by a unique key.
     * <br>
     * Example:
     * <pre>
     * public PregelSchema schema(PregelConfig config) {
     *      return new PregelSchema.Builder()
     *          .add("key", ValueType.LONG)
     *          .add("privateKey", ValueType.LONG, Visibility.PRIVATE)
     *          .build();
     * }
     * </pre>
     *
     * @see PregelSchema
     */
    PregelSchema schema(C config);

    /**
     * The init method is called in the beginning of the first
     * superstep (iteration) of the Pregel computation and allows
     * initializing node values.
     * <br>
     * The context parameter provides access to node properties of
     * the in-memory graph and the algorithm configuration.
     */
    default void init(InitContext<C> context) {}

    /**
     * The masterCompute method is called exactly once after every superstep.
     * It is called by a single thread.
     *
     * @return true, iff the computation converged and should stop
     */
    default boolean masterCompute(MasterComputeContext<C> context) {
        return false;
    }

    /**
     * A reducer is used to combine messages sent to a single node. Based on
     * the reduce function, multiple messages are condensed into a single one.
     * Use cases are computing the sum, count, minimum or maximum of messages.
     *
     * Specifying a reducer can significantly reduce memory consumption and
     * runtime of the computation.
     */
    default Optional<Reducer> reducer() {
        return Optional.empty();
    }

    /**
     * If the input graph is weighted, i.e. relationships have a
     * property, this method can be overridden to apply that weight
     * on a message before it is read by the receiving node.
     * <br>
     * If the input graph has no relationship properties, i.e. is
     * unweighted, the method is skipped.
     */
    default double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
        return nodeValue;
    }

    /**
     * The close method is called at the very end of the computation,
     * after the end result has been produced and no more work is being
     * done.
     * <br>
     * Implement this method to close any resources that the computation opened,
     * for example (Closable)ThreadLocals.
     */
    default void close() {}
}
