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
package org.openjdk.jol.info;

import org.openjdk.jol.util.ObjectUtils;
import org.openjdk.jol.util.SimpleIdentityHashSet;
import org.openjdk.jol.util.SimpleStack;
import org.openjdk.jol.vm.VM;

import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * Concrete class to walk object graphs.
 * <p>
 * Note: This walker is modified to skip record types on the object graph.
 *
 * @author Aleksey Shipilev
 * @author s1ck
 * @author chokladmunk
 */
public class RecordlessGraphWalker extends AbstractGraphWalker {

    private final GraphVisitor[] visitors;
    private final HashMap<Class<?>, Long> sizeCache;

    public RecordlessGraphWalker(GraphVisitor... visitor) {
        this.visitors = visitor;
        sizeCache = new HashMap<>();
    }

    public GraphLayout walk(Object... roots) {
        verifyRoots(roots);

        GraphLayout data = new GraphLayout(roots);

        SimpleIdentityHashSet visited = new SimpleIdentityHashSet();
        SimpleStack<GraphPathRecord> s = new SimpleStack<>();

        int rootId = 1;
        boolean single = (roots.length == 1);
        for (Object root : roots) {
            String label = single ? "" : ("<r" + rootId + ">");
            GraphPathRecord e = new FieldGraphPathRecord(null, label, 0, root);
            if (visited.add(root)) {
                data.addRecord(e);
                s.push(e);
            }
            rootId++;
        }

        while (!s.isEmpty()) {
            GraphPathRecord cGpr = s.pop();
            Object o = cGpr.obj();
            Class<?> cl = o.getClass();

            if (cl.isArray()) {
                if (cl.getComponentType().isPrimitive()) {
                    // Nothing to do here
                    continue;
                }

                Object[] arr = (Object[]) o;

                for (int i = 0; i < arr.length; i++) {
                    Object e = arr[i];
                    if (e != null && visited.add(e)) {
                        GraphPathRecord gpr = new ArrayGraphPathRecord(cGpr, i, cGpr.depth() + 1, e);
                        gpr.setSize(sizeOf(e));
                        data.addRecord(gpr);
                        for (GraphVisitor v : visitors) {
                            v.visit(gpr);
                        }
                        s.push(gpr);
                    }
                }
            } else {
                // current (parent)
                cGpr.setSize(sizeOf(o));
                // children
                for (Field f : getAllReferenceFields(cl)) {
                    Object e = ObjectUtils.value(o, f);
                    if (e != null && visited.add(e)) {
                        GraphPathRecord gpr = new FieldGraphPathRecord(cGpr, f.getName(), cGpr.depth() + 1, e);
                        gpr.setSize(sizeOf(e));
                        data.addRecord(gpr);
                        for (GraphVisitor v : visitors) {
                            v.visit(gpr);
                        }
                        s.push(gpr);
                    }
                }
            }
        }
        return data;
    }

    private long sizeOf(Object o) {
        var size = this.sizeCache.get(o.getClass());
        if (size == null) {
            if (o.getClass().isRecord()) {
                // Jol only supports calling sizeOf for record types if
                // jol.magicFieldOffset is enabled. However, this leads
                // to potential problems, such as long execution times
                // and the flag is generally only recommended for one-offs
                // and not for production-use.
                // We instead just use a fixed size for record types, which
                // might be very well underestimated.
                size = (long) VM.current().objectHeaderSize();
            } else {
                size = VM.current().sizeOf(o);
            }

            this.sizeCache.put(o.getClass(), size);
        }
        return size;
    }
}
