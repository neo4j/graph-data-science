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
package org.neo4j.graphalgo.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public final class GraphNegativeTest extends RandomGraphTestCase {

    private Class<? extends GraphFactory> graphImpl;

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    @SuppressWarnings("unchecked")
    public GraphNegativeTest(
            Class<?> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = (Class<? extends GraphFactory>) graphImpl;
    }

    @Test
    public void shouldThrowForNonExistingStringLabel() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Node label not found: 'foo'");
        new GraphLoader(RandomGraphTestCase.db)
                .withLabel("foo")
                .load(graphImpl);
    }

    @Test
    public void shouldThrowForNonExistingLabel() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Node label not found: 'foo'");
        new GraphLoader(RandomGraphTestCase.db)
                .withLabel(Label.label("foo"))
                .load(graphImpl);
    }

    @Test
    public void shouldThrowForNonExistingStringRelType() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Relationship type(s) not found: 'foo'");
        new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType("foo")
                .load(graphImpl);
    }

    @Test
    public void shouldThrowForNonExistingRelType() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Relationship type(s) not found: 'foo'");
        new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType(RelationshipType.withName("foo"))
                .load(graphImpl);
    }

    @Test
    public void shouldThrowForNonExistingNodeProperty() {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Node property not found: 'foo'");
        new GraphLoader(RandomGraphTestCase.db)
                .withOptionalNodeProperties(new PropertyMapping("foo", "foo", 0.0))
                .load(graphImpl);
    }

}
