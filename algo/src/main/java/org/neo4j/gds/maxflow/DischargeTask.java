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
package org.neo4j.gds.maxflow;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DischargeTask implements Runnable {
    private final FlowGraph flowGraph;
    private final HugeDoubleArray excess;
    private final HugeLongArray label;
    private final HugeLongArray tempLabel;
    private final AtomicWorkingSet workingSet;
    private final HugeAtomicDoubleArray addedExcess;
    private final HugeAtomicBitSet isDiscovered;
    private final long targetNode;
    private final long beta;
    private final AtomicLong workSinceLastGR;

    private final long batchSize;
    private final long nodeCount;
    private final HugeLongArrayQueue localDiscoveredVertices;
    private PHASE phase;
    private long localWork;

    public DischargeTask(
        FlowGraph flowGraph,
        HugeDoubleArray excess,
        HugeLongArray label,
        HugeLongArray tempLabel,
        HugeAtomicDoubleArray addedExcess,
        HugeAtomicBitSet isDiscovered,
        AtomicWorkingSet workingSet,
        long targetNode,
        long beta,
        AtomicLong workSinceLastGR
    ) {
        this.excess = excess;
        this.flowGraph = flowGraph;
        this.label = label;
        this.tempLabel = tempLabel;
        this.addedExcess = addedExcess;
        this.isDiscovered = isDiscovered;
        this.workingSet = workingSet;
        this.targetNode = targetNode;
        this.beta = beta;
        this.workSinceLastGR = workSinceLastGR;

        this.batchSize = 8;
        this.phase = PHASE.DISCHARGE;

        this.localWork = 0;
        this.nodeCount = flowGraph.nodeCount();
        this.localDiscoveredVertices = HugeLongArrayQueue.newQueue(flowGraph.nodeCount());
    }

    public void run() {
        switch (phase) {
            case PHASE.DISCHARGE -> dischargeWorkingSet();
            case PHASE.SYNC_WORKING_SET -> syncWorkingSet();
            case PHASE.UPDATE_WORKING_SET -> updateAndSyncNewWorkingSet();
        }
    }

    private void batchConsumeWorkingSet(Consumer<Long> consumer) {
        long oldIdx;
        while ((oldIdx = workingSet.getAndAdd(batchSize)) < workingSet.size()) {
            long toIdx = Math.min(oldIdx + batchSize, workingSet.size());
            workingSet.consumeBatch(oldIdx, toIdx, consumer);
        }
    }

    private void dischargeWorkingSet() {
        batchConsumeWorkingSet(this::discharge);
        phase = PHASE.SYNC_WORKING_SET;
    }

    void discharge(long v) {
        if (label.get(v) >= nodeCount || v == targetNode) {
            return;
        }

        tempLabel.set(v, label.get(v));
        final var e = new MutableDouble(excess.get(v));

        while (e.doubleValue() > 0) {
            final var newLabel = new MutableLong(nodeCount);
            final var breakOuter = new MutableBoolean(false);

            //todo: Check to improve zero comparisons!

            ResidualEdgeConsumer consumer = (long s, long t, long relIdx, double residualCapacity, boolean isReverse) -> {
                if (residualCapacity <= 0.0) {
                    return true; //skip
                }
                var admissible = (tempLabel.get(s) == label.get(t) + 1);
                if (admissible) {
                    if (excess.get(t) > 0.0) {
                        boolean win = (label.get(s) == label.get(t) + 1 || label.get(s) + 1 < label.get(t) || (label.get(
                            s) == label.get(t) && s < t));
                        if (!win) {
                            breakOuter.setTrue();
                            return true;
                        }
                    }
                    var delta = Math.min(e.doubleValue(), residualCapacity);
                    flowGraph.push(relIdx, delta, isReverse);
                    e.subtract(delta);
                    addedExcess.getAndAdd(t, delta);

                    if (!isDiscovered.getAndSet(t)) {
                        localDiscoveredVertices.add(t);
                    }
                    if (e.doubleValue() <= 0.0) {
                        breakOuter.setTrue();
                        return false;
                    }
                } else if (label.get(t) >= tempLabel.get(s)) {
                    newLabel.setValue(Math.min(newLabel.longValue(), label.get(t) + 1));
                    //if ws are sorted by label ascendingly, then later values will be neither better(lower) than this, nor admissible -> return false; (break)
                }
                return true;
            };

            flowGraph.forEachRelationship(v, consumer);

            if (breakOuter.isTrue()) {
                break;
            }
            tempLabel.set(v, newLabel.longValue());
            localWork += flowGraph.outDegree(v) + beta;
            if (tempLabel.get(v) == nodeCount) {
                break;
            }
        }
        addedExcess.getAndAdd(v, (e.doubleValue() - excess.get(v)));
        if (e.doubleValue() > 0.0 && !isDiscovered.getAndSet(v)) {
            localDiscoveredVertices.add(v);
        }
    }

    void syncWorkingSet() {
        batchConsumeWorkingSet((v) -> {
            label.set(v, tempLabel.get(v));
            excess.addTo(v, addedExcess.get(v));
            addedExcess.set(v, 0);
            isDiscovered.clear(v);
        });
        workSinceLastGR.addAndGet(localWork);
        localWork = 0;
        phase = PHASE.UPDATE_WORKING_SET;
    }

    void updateAndSyncNewWorkingSet() {
        workingSet.batchPushAndConsume(localDiscoveredVertices, (v) -> {
            excess.addTo(v, addedExcess.get(v));
            addedExcess.set(v, 0);
            isDiscovered.clear(v);
        });
        phase = PHASE.DISCHARGE;
    }

    enum PHASE {
        DISCHARGE,
        SYNC_WORKING_SET,
        UPDATE_WORKING_SET,
    }
}
