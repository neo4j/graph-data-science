/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.graphalgo.core.utils.RawValues;

final class IntIntSubWeights extends SubWeights {

  private final LongDoubleScatterMap weights;

  IntIntSubWeights(int nodeCount) {
    weights = new LongDoubleScatterMap(nodeCount);
  }

  void add(final int source, final int target, final double weight) {
    weights.addTo(RawValues.combineIntInt(source, target), weight);
    weights.addTo(RawValues.combineIntInt(target, source), weight);
  }

  @Override
  double getOrDefault(final long source, final long target) {
    return weights.getOrDefault(RawValues.combineIntInt((int) source, (int) target), 0.0);
  }

  @Override
  void release() {
    weights.release();
  }
}
