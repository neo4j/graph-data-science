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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;

public class CollectingConsumer implements RelationshipConsumer, RelationshipWithPropertyConsumer {

    public final LongArrayList targets = new LongArrayList();
    public final DoubleArrayList properties = new DoubleArrayList();

    @Override
    public boolean accept(long sourceNodeId, long targetNodeId) {
        targets.add(targetNodeId);
        return true;
    }

    @Override
    public boolean accept(long sourceNodeId, long targetNodeId, double property) {
        targets.add(targetNodeId);
        properties.add(property);
        return true;
    }

    public void clear() {
        this.targets.clear();
        this.properties.clear();
    }
}
