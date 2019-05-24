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

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.unionfind.UnionFindAlgo;
import org.neo4j.graphalgo.impl.unionfind.UnionFindProcExec;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class UnionFindProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.unionFind", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{weightProperty:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition'}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindProcExec.UnionFindResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.run(
                config( config, label, relationship ),
                this::seqQueue );
    }

    @Procedure(value = "algo.unionFind.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{weightProperty:'propertyName', threshold:0.42, defaultValue:1.0) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.stream(
                config( config, label, relationship ),
                this::seqQueue );
    }

    @Procedure(value = "algo.unionFind.queue", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition',concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindProcExec.UnionFindResult> unionFindQueue(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.run(
                config( config, label, relationship ),
                this::seqQueue
        );
    }

    @Procedure(value = "algo.unionFind.queue.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindQueueStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.stream(
                config( config, label, relationship ),
                this::seqQueue );
    }

    @Procedure(value = "algo.unionFind.forkJoinMerge", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition', concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindProcExec.UnionFindResult> unionFindForkJoinMerge(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.run(
                config( config, label, relationship ),
                this::seqMerge
        );
    }

    @Procedure(value = "algo.unionFind.forkJoinMerge.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindForkJoinMergeStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.stream(
                config( config, label, relationship ),
                this::seqMerge
        );
    }

    @Procedure(value = "algo.unionFind.forkJoin", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition',concurrency:4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindProcExec.UnionFindResult> unionFindForkJoin(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.run(
                config( config, label, relationship ),
                this::seqForkJoin
        );
    }

    @Procedure(value = "algo.unionFind.forkJoin.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{property:'propertyName', threshold:0.42, defaultValue:1.0,concurrency:4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindForJoinStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return UnionFindProcExec.stream(
                config( config, label, relationship ),
                this::seqForkJoin
        );
    }

    private UnionFindProcExec seqForkJoin() {
        return new UnionFindProcExec(
                api,
                log,
                transaction,
                UnionFindAlgo.SEQ,
                UnionFindAlgo.FORK_JOIN
        );
    }

    private UnionFindProcExec seqMerge() {
        return new UnionFindProcExec(
                api,
                log,
                transaction,
                UnionFindAlgo.SEQ,
                UnionFindAlgo.FJ_MERGE
        );
    }

    private UnionFindProcExec seqQueue() {
        return new UnionFindProcExec(
                api,
                log,
                transaction,
                UnionFindAlgo.SEQ,
                UnionFindAlgo.QUEUE
        );
    }

    private ProcedureConfiguration config( Map<String,Object> config, String label, String relationship )
    {
        return new ProcedureConfiguration(config)
                .overrideNodeLabelOrQuery( label )
                .overrideRelationshipTypeOrQuery( relationship );
    }
}
