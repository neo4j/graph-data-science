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
package org.neo4j.graphalgo.unionfind;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphalgo.impl.wcc.WCC;
import org.neo4j.graphalgo.impl.wcc.WCCType;
import org.neo4j.graphalgo.wcc.WccBaseProc;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class UnionFindProc<T extends WCC<T>> extends WccBaseProc<T> {

    @Deprecated
    @Procedure(value = "algo.unionFind", mode = Mode.WRITE, deprecatedBy = "algo.wcc")
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{weightProperty: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', consecutiveId: false}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.PARALLEL);
    }

    @Deprecated
    @Procedure(name = "algo.unionFind.stream", mode = READ, deprecatedBy = "algo.wcc.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
                 "{weightProperty: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', consecutiveId: false}}} " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.PARALLEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.memrec", mode = READ, deprecatedBy = "algo.wcc.memrec")
    @Description("CALL algo.unionFind.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for UnionFind")
    public Stream<MemRecResult> unionFindMemRec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Procedure(value = "algo.beta.unionFind.pregel", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
                 "{weightProperty: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', consecutiveId: false}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindPregel(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, UnionFindType.PREGEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.queue", mode = Mode.WRITE, deprecatedBy = "algo.wcc")
    @Description("CALL algo.unionFind.queue(label:String, relationship:String, " +
                 "{property: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindQueue(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.PARALLEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.queue.stream", mode = READ, deprecatedBy = "algo.wcc.stream")
    @Description("CALL algo.unionFind.queue.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindQueueStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.PARALLEL);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoinMerge", mode = Mode.WRITE, deprecatedBy = "algo.wcc")
    @Description("CALL algo.unionFind.forkJoinMerge(label:String, relationship:String, " +
                 "{property: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindForkJoinMerge(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.FJ_MERGE);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoinMerge.stream", mode = READ, deprecatedBy = "algo.wcc.stream")
    @Description("CALL algo.unionFind.forkJoinMerge.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindForkJoinMergeStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.FJ_MERGE);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoin", mode = Mode.WRITE, deprecatedBy = "algo.wcc")
    @Description("CALL algo.unionFind.forkJoin(label:String, relationship:String, " +
                 "{property: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> unionFindForkJoin(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.FORK_JOIN);
    }

    @Deprecated
    @Procedure(value = "algo.unionFind.forkJoin.stream", mode = READ, deprecatedBy = "algo.wcc.stream")
    @Description("CALL algo.unionFind.forkJoin.stream(label:String, relationship:String, " +
                 "{property: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', concurrency: 4}) " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> unionFindForJoinStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.FORK_JOIN);
    }

    @Override
    protected String name() {
        return "UnionFind";
    }
}
