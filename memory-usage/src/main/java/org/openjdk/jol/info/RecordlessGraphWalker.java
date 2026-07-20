/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
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
                        data.addRecord(gpr);
                        for (GraphVisitor v : visitors) {
                            v.visit(gpr);
                        }
                        s.push(gpr);
                    }
                }
            } else {
                Long knownSize = sizeCache.get(cl);
                if (knownSize == null) {
                    if (cl.isRecord()) {
                        knownSize = (long) VM.current().objectHeaderSize();
                    } else {
                        knownSize = VM.current().sizeOf(o);
                    }
                    sizeCache.put(cl, knownSize);
                }
                cGpr.setSize(knownSize);

                for (Field f : getAllReferenceFields(cl)) {
                    Object e = ObjectUtils.value(o, f);
                    if (e != null && visited.add(e)) {
                        GraphPathRecord gpr = new FieldGraphPathRecord(cGpr, f.getName(), cGpr.depth() + 1, e);
                        if (e.getClass().isRecord()) {
                            gpr.setSize(VM.current().objectHeaderSize());
                        }
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
}
