/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;

import java.util.Optional;

class WccMutateProcTest extends WccBaseProcTest<WccWriteConfig> implements GraphMutationTest<WccWriteConfig, DisjointSetStruct> {

    private static final String WRITE_PROPERTY = "componentId";

    @Override
    public Class<? extends AlgoBaseProc<?, DisjointSetStruct, WccWriteConfig>> getProcedureClazz() {
        return WccMutateProc.class;
    }

    @Override
    public WccWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccWriteConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", WRITE_PROPERTY);
        }
        return mapWrapper;
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a {componentId: 0})" +
            ", (b {componentId: 0})" +
            ", (c {componentId: 0})" +
            ", (d {componentId: 0})" +
            ", (e {componentId: 1})" +
            ", (f {componentId: 1})" +
            ", (g {componentId: 1})" +
            ", (h {componentId: 2})" +
            ", (i {componentId: 2})" +
            ", (j {componentId: 3})" +
            // {A, B, C, D}
            ", (a)-[{w: 1.0d}]->(b)" +
            ", (b)-[{w: 1.0d}]->(c)" +
            ", (c)-[{w: 1.0d}]->(d)" +
            ", (d)-[{w: 1.0d}]->(e)" +
            // {E, F, G}
            ", (e)-[{w: 1.0d}]->(f)" +
            ", (f)-[{w: 1.0d}]->(g)" +
            // {H, I}
            ", (h)-[{w: 1.0d}]->(i)";
    }
}
