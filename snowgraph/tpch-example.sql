/*
Copyright (c) "Neo4j"
Neo4j Sweden AB [http://neo4j.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


-- *========================================*
-- | Neo4j Graph Data Science for Snowflake |
-- | Basket analysis example on TPC-H data  |
-- *========================================*

-- In this example, we will use Neo4j Graph Data Science (GDS) for Snowflake to perform basket analysis on the TPC-H dataset.
-- For that we will look at parts that are often ordered together and use the node similarity algorithm to find similar parts.
-- We will then write the results back to Snowflake and query the results.
--
-- See https://docs.snowflake.com/en/user-guide/sample-data-tpch for information about the sample data, its schema.
-- NOTE: You may need to install the dataset if it's not already present in your account.
--
-- For this example to run, we make the following assumptions:
--
-- * Neo4j Graph Data Science application is installed correctly and called "Neo4j_GDS"
-- * Neo4j Graph Data Science application has been granted the CREATE COMPUTE POOL privilege
-- * Neo4j Graph Data Science application has been granted the CREATE WAREHOUSE privilege
-- * The current role can create databases and schemas
-- * The current role has granted the application role Neo4j_GDS.app_user
-- * The current role has access to the Snowflake sample data set (database "snowflake_sample_data")
--
-- The example is split into three parts:
-- 1. Data preparation,
-- 2. Application setup, and
-- 3. Graph analysis.
-- The first two parts need to be executed only once, while the third part can be executed multiple times with different parameters or algorithms.


-- ==================================================
-- 1. Data preparation
-- ==================================================
-- Create a database which we will use to prepare data for GDS.
CREATE DATABASE IF NOT EXISTS tpch_example;
CREATE SCHEMA IF NOT EXISTS tpch_example.gds;
USE SCHEMA tpch_example.gds;

-- GDS reads data from tables that represent nodes and relationships.
-- Nodes are usually represented by entity tables, like persons or products.
-- Relationships are foreign keys between entity tables (1:1, 1:n) or via mapping tables (n:m).
-- In addition, GDS expects certain naming conventions on column names.
-- If the data is not yet in the right format, we can use views to get there.
--
-- For our analysis, we will use two different types of nodes: parts and orders.
-- We want to find similar parts by looking at the orders in which they appeared.
-- The relationships will be the line items linking a part to an order.
-- The result will be a new table containing pairs of parts including their similarity score.

-- We start by creating two views to represent our node tables.
-- GDS requires a node table to contain a 'nodeId' column.
-- Since we do not need any node properties, this will be the only column we project.
-- Note, that the `nodeId` column is used to uniquely identify a node in the table.
-- The uniqueness is usually achieved by using the primary key in that table, here 'p_partkey'.
CREATE OR REPLACE VIEW parts (nodeId) AS
SELECT p.p_partkey AS nodeId FROM snowflake_sample_data.tpch_sf1.part p;

-- We do the same for the orders by projecting the `o_orderkey` to 'nodeId'.
CREATE OR REPLACE VIEW orders (nodeId) AS
SELECT o.o_orderkey AS nodeId FROM snowflake_sample_data.tpch_sf1.orders o;

-- The line items represent the relationship between parts and orders.
-- GDS requires a `sourceNodeId` and a `targetNodeId` column to identify.
-- Here, a part is the source of a relationship and an order is the target.
CREATE OR REPLACE VIEW part_in_order(sourceNodeId, targetNodeId) AS
SELECT
    l.l_partkey AS sourceNodeId,
    l.l_orderkey AS targetNodeId
FROM snowflake_sample_data.tpch_sf1.lineitem l;

-- We have now prepared the data for GDS.

-- ==================================================
-- 2. Application setup
-- ==================================================

-- We start by switching to the Neo4j_GDS application.
USE DATABASE Neo4j_GDS;

-- Next, we want to consider the warehouse that the GDS application will use to execute queries.
-- For this example a MEDIUM size warehouse, so we configure the application's warehouse accordingly
ALTER WAREHOUSE Neo4j_GDS_app_warehouse SET WAREHOUSE_SIZE='MEDIUM';
-- A highly performant warehouse can speed up graph projections but does not affect algorithm computation.
-- Especially if the views are more complex than shown in this example, a more performant warehouse is beneficial.
-- The warehouse can then be brought back to a less expensive configuration after the projection is done.
-- ALTER WAREHOUSE Neo4j_GDS_app_warehouse
--   WAREHOUSE_SIZE='X-SMALL';

-- The following grants are necessary for the GDS application to read and write data.
-- The next queries are required to read from our prepared views.
GRANT USAGE ON DATABASE             tpch_example     TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA               tpch_example.gds TO APPLICATION Neo4j_GDS;
GRANT SELECT ON ALL VIEWS IN SCHEMA tpch_example.gds TO APPLICATION Neo4j_GDS;
-- This grant is necessary to enable write back of algorithm results.
GRANT CREATE TABLE ON SCHEMA        tpch_example.gds TO APPLICATION Neo4j_GDS;

-- We have now prepared the environment to properly run the GDS application and can start with our analysis.
-- Note, that data preparation and application setup only need to be done once.

-- Our final preparation is to select a compute pool to run the GDS service.
-- Available compute pools to select from are:
-- * CPU_X64_XS
-- * CPU_X64_M
-- * CPU_X64_L
-- * HIGHMEM_X64_S
-- * HIGHMEM_X64_M
-- * HIGHMEM_X64_L
-- * GPU_NV_S
--
-- For our example, we use a large compute pool as the node similarity algorithm is computationally intensive, but without extra memory because the graph is quite small.
-- We select: CPU_X64_L

-- ==================================================
-- 3. Graph analysis
-- ==================================================

-- The first step is to create a new GDS session.
-- Creating the session will start a container service on the selected compute pool.
-- In addition, all the service functions that allow us to interact with the GDS service are created.
-- A session can be used by many users, but only one session can be active at a time.
CALL gds.create_session('CPU_X64_L');

-- Once the session is started, we can project our node and relationship views into a GDS in-memory graph.
-- The graph will be identified by the name "parts_in_orders".
-- The mandatory parameters are the node tables and the relationship tables.
-- A node table mapping points from a table/view to a node label that is used in the GDS graph.
-- For example, the rows of 'tpch_example.gds.parts' will be nodes labeles as 'Part'.
-- Relationship tables need a bit more configuration.
-- Besides the type that is used in the GDS graph, here 'PART_IN_ORDER', we also need to specify source and target tables.
-- We also specify the optional read concurrency to optimize building the graph projection.
-- The concurrency can be set to the number of cores available on the compute pool node.
SELECT gds.graph_project('parts_in_orders', {
    'nodeTables': {
        'tpch_example.gds.parts':  'Part',
        'tpch_example.gds.orders': 'Order'
    },
    'relationshipTables': {
        'tpch_example.gds.part_in_order': {
            'type': 'PART_IN_ORDER',
            'sourceTable': 'tpch_example.gds.parts',
            'targetTable': 'tpch_example.gds.orders',
            'orientation':  'NATURAL'
        }
    },
    'readConcurrency':   28
});

-- The graph we project is a so-called bipartite graph, as it contains two types of nodes and all relationships point from one type to the other.
-- The node similarity algorithm looks at all pairs of nodes of the first type and calculates the similarity for each pair based on common relationships.
-- In our case, the algorithm will calculate the similarity between two parts based on the orders in which they appear.
-- The algorithm produces new relationships between parts, the relationship property is the similarity score.
-- For further information on the node similarity algorithm, please refer to the GDS documentation:
-- https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/
SELECT gds.node_similarity('parts_in_orders', {
    'mutateRelationshipType': 'SIMILAR_TO',
    'mutateProperty':         'similarity',
    'concurrency':            28
});

-- Once the algorithm has finished, we can write the results back to Snowflake tables for further analysis.
-- We want to write back the similarity relationships between parts.
-- The specified table will contain the original source and target node ids and the similarity score.
SELECT gds.write_relationships('parts_in_orders', {
    'sourceLabel':          'Part',
    'targetLabel':          'Part',
    'relationshipType':     'SIMILAR_TO',
    'relationshipProperty': 'similarity',
    'table':                'tpch_example.gds.part_similar_to_part'
});

-- After writing the table, we need to ensure that our current role is allowed to read it.
-- Alternatively, we can also grant access to all future tables created by the application.
GRANT SELECT ON tpch_example.gds.part_similar_to_part TO ROLE <your_role>; 

-- Since the results are now stored in Snowflake, we can query them and join them with our original data.
-- For example, we can find the names of the most similar parts based on the similarity score.
-- Simply speaking, this could be used as a recommendation system for parts.
SELECT DISTINCT p_source.p_name, p_target.p_name, sim.similarity
FROM snowflake_sample_data.tpch_sf1.part p_source
    JOIN tpch_example.gds.part_similar_to_part sim
        ON p_source.p_partkey = sim.sourcenodeid
    JOIN snowflake_sample_data.tpch_sf1.part p_target
        ON p_target.p_partkey = sim.targetnodeid
ORDER BY sim.similarity DESC LIMIT 10;

-- The GDS service is a long-running service and should be stopped when not in use.
-- Once we completed our analysis, we can stop the session, which suspends the container service.
-- We can restart the session at any time to continue our analysis.
CALL gds.stop_session();
