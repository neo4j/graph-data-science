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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphalgo.impl.wcc.WCC;
import org.neo4j.graphalgo.impl.wcc.WCCType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class WccProc<T extends WCC<T>> extends WccBaseProc<T> {

    @Procedure(value = "algo.wcc", mode = Mode.WRITE)
    @Description("CALL algo.wcc(label:String, relationship:String, " +
                 "{weightProperty: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', consecutiveId: false}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> wcc(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.PARALLEL);
    }

    @Procedure(value = "algo.wcc.stream")
    @Description("CALL algo.wcc.stream(label:String, relationship:String, " +
                 "{weightProperty: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', consecutiveId: false}}} " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> wccStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.PARALLEL);
    }

    @Procedure(value = "algo.wcc.memrec", mode = Mode.READ)
    @Description("CALL algo.wcc.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for WCC")
    public Stream<MemRecResult> wccMemRec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Procedure(value = "algo.beta.wcc", mode = Mode.WRITE)
    @Description("CALL algo.beta.wcc(label:String, relationship:String, " +
                 "{weightProperty: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', consecutiveId: false}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> betaWcc(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.PARALLEL);
    }

    @Procedure(value = "algo.beta.wcc.stream")
    @Description("CALL algo.beta.wcc.stream(label:String, relationship:String, " +
                 "{weightProperty: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', consecutiveId: false}}} " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> betaWccStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.PARALLEL);
    }

    @Procedure(value = "algo.beta.wcc.memrec", mode = Mode.READ)
    @Description("CALL algo.beta.wcc.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for WCC")
    public Stream<MemRecResult> betaWccMemRec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Procedure(value = "algo.beta.wcc.pregel", mode = Mode.WRITE)
    @Description("CALL algo.beta.wcc.pregel(label:String, relationship:String, " +
                 "{weightProperty: 'weight', threshold: 0.42, defaultValue: 1.0, write: true, writeProperty: 'community', seedProperty: 'seedCommunity', consecutiveId: false}) " +
                 "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<WriteResult> betaWccPregel(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return run(label, relationship, config, WCCType.PREGEL);
    }

    @Procedure(value = "algo.beta.wcc.pregel.stream")
    @Description("CALL algo.beta.wcc.pregel.stream(label:String, relationship:String, " +
                 "{weightProperty: 'propertyName', threshold: 0.42, defaultValue: 1.0, seedProperty: 'seedCommunity', consecutiveId: false}}} " +
                 "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<StreamResult> betaWccPregelStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationship, config, WCCType.PREGEL);
    }

    @Procedure(value = "algo.beta.wcc.pregel.memrec", mode = Mode.READ)
    @Description("CALL algo.beta.wcc.pregel.memrec(label:String, relationship:String, {...properties}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for WCC")
    public Stream<MemRecResult> betaWccPregelMemRec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Override
    protected String name() {
        return "WCC";
    }
}
