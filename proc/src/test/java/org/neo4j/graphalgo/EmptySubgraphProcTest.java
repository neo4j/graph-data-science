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
package org.neo4j.graphalgo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class EmptySubgraphProcTest {

    private static final String DB_CYPHER = "" +
            "CREATE (a:A {id: 0}) " +
            "CREATE (b:B {id: 1}) " +
            "CREATE (a)-[:X]->(b)";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(UnionFindProc.class);
        procedures.registerProcedure(MSColoringProc.class);
        procedures.registerProcedure(StronglyConnectedComponentsProc.class);
        procedures.registerProcedure(BetweennessCentralityProc.class);
        procedures.registerProcedure(ClosenessCentralityProc.class);
        procedures.registerProcedure(TriangleProc.class);
        procedures.registerProcedure(DangalchevCentralityProc.class);
        procedures.registerProcedure(HarmonicCentralityProc.class);
        procedures.registerProcedure(KSpanningTreeProc.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        procedures.registerProcedure(LouvainProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(PrimProc.class);

        db.execute(DB_CYPHER);
    }

    @AfterClass
    public static void tearDown() {
        if (db != null) db.shutdown();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testUnionFindStream() {
        Result result = db.execute(String.format("CALL algo.unionFind.stream('C', '',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.unionFind.stream('', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.unionFind.stream('C', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFindMSColoringStream() {
        Result result = db.execute(String.format("CALL algo.unionFind.mscoloring.stream('C', '',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.unionFind.mscoloring.stream('C', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.unionFind.mscoloring.stream('C', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponentsStream() {
        Result result = db.execute(String.format("CALL algo.scc.stream('C', '',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.stream('', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.stream('C', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponentsMultiStepStream() {
        Result result = db.execute(String.format("CALL algo.scc.stream('C', '',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.stream('', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.stream('C', 'Y',{graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponentsTunedTarjanStream() {
        Result result = db.execute(String.format("CALL algo.scc.recursive.tunedTarjan.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.recursive.tunedTarjan.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.recursive.tunedTarjan.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testForwardBackwardStronglyConnectedComponentsStream() {
        Result result = db.execute(String.format("CALL algo.scc.forwardBackward.stream(0, 'C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.forwardBackward.stream(0, '', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.scc.forwardBackward.stream(0, 'C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testBetweennessCentralityStream() {
        Result result = db.execute(String.format("CALL algo.betweenness.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.betweenness.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.betweenness.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testSampledBetweennessCentralityStream() {
        Result result = db.execute(String.format("CALL algo.betweenness.sampled.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.betweenness.sampled.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.betweenness.sampled.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testClosenessCentralityStream() {
        Result result = db.execute(String.format("CALL algo.closeness.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.closeness.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.closeness.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testTriangleCountStream() {
        Result result = db.execute(String.format("CALL algo.triangleCount.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.triangleCount.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.triangleCount.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testTriangleStream() {
        Result result = db.execute(String.format("CALL algo.triangle.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.triangle.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.triangle.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testDangelchevCentralityStream() {
        Result result = db.execute(String.format("CALL algo.closeness.dangalchev.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.closeness.dangalchev.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.closeness.dangalchev.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testHarmonicCentralityStream() {
        Result result = db.execute(String.format("CALL algo.closeness.harmonic.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.closeness.harmonic.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.closeness.harmonic.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testLabelPropagationStream() {
        Result result = db.execute(String.format("CALL algo.labelPropagation.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.labelPropagation.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.labelPropagation.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testLouvainStream() {
        Result result = db.execute(String.format("CALL algo.louvain.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.louvain.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.louvain.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }

    @Test
    public void testPageRankStream() {
        Result result = db.execute(String.format("CALL algo.pageRank.stream('C', '', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.pageRank.stream('', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
        result = db.execute(String.format("CALL algo.pageRank.stream('C', 'Y', {graph:'%s'})", graphImpl));
        assertFalse(result.hasNext());
    }
}
