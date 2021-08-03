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
package org.neo4j.gds.datasets;

public final class Cora extends AbstractCora {

    public static final String ID = "Cora";

    Cora() {
        super(ID);
    }

    @Override
    String coraNodesFile() {
        return "cora.content";
    }

    @Override
    String trainRelationshipsFile() {
        return "cora.train.csv";
    }

    @Override
    String testRelationshipsFile() {
        return "cora.test.csv";
    }

    @Override
    String citesRelationshipsFile() {
        return "cora.cites";
    }

    static int numberOfFeatures() {
        return 1433;
    }
}
