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
package org.neo4j.graphalgo.core.graphbuilder;

import org.junit.Test;
import org.neo4j.graphalgo.Neo4jTestCase;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
public class GraphBuilderTest extends Neo4jTestCase {

    @Test
    public void testRingBuilder() throws Exception {

        Runnable mock = mock(Runnable.class);
        GraphBuilder.create((GraphDatabaseAPI) db)
                .newRingBuilder()
                .setRelationship(RELATION)
                .createRing(10)
                .forEachNodeInTx(node -> {
                    assertEquals(2, node.getDegree());
                    mock.run();
                });
        verify(mock, times(10)).run();

    }
}
