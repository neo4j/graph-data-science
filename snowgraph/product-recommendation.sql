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
-- For that, we will look at parts that are often ordered together and use the node similarity algorithm to find similar parts.
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
CREATE DATABASE IF NOT EXISTS product_recommendation;
CREATE SCHEMA IF NOT EXISTS product_recommendation.gds;
USE SCHEMA product_recommendation.gds;

-- GDS reads data from tables that represent nodes and relationships.
-- Nodes are usually represented by entity tables, like persons or products.
-- Relationships are foreign keys between entity tables (1:1, 1:n) or via mapping tables (n:m).
-- In addition, GDS expects certain naming conventions on column names.
-- If the data is not yet in the right format, we can use views to get there.

-- For our analysis, we will use two different types of nodes: parts and orders.
-- We want to find similar parts by looking at the orders in which they appeared.
-- The relationships will be the line items linking a part to an order.
-- The result will be a new table containing pairs of parts including their similarity score.

-- We start by creating two views to represent our node tables.
-- GDS requires a node table to contain a 'nodeId' column.
-- Since we do not need any node properties, this will be the only column we project.
-- Note that the `nodeId` column is used to uniquely identify a node in the table.
-- The uniqueness is usually achieved by using the primary key in that table, here 'p_partkey'.
CREATE OR REPLACE VIEW parts AS
SELECT p_partkey AS nodeId FROM snowflake_sample_data.tpch_sf1.part;

-- We do the same for the orders by projecting the `o_orderkey` to 'nodeId'.
CREATE OR REPLACE VIEW orders AS
SELECT o_orderkey AS nodeId FROM snowflake_sample_data.tpch_sf1.orders;

-- The line items represent the relationship between parts and orders.
-- GDS requires a `sourceNodeId` and a `targetNodeId` column to identify.
-- Here, a part is the source of a relationship and an order is the target.
CREATE OR REPLACE VIEW part_in_order AS
SELECT
    l_partkey AS sourceNodeId,
    l_orderkey AS targetNodeId
FROM snowflake_sample_data.tpch_sf1.lineitem;

-- We have now prepared the data for GDS.


-- ==================================================
-- 2. Application setup
-- ==================================================

-- We start by switching to the Neo4j_GDS application.
USE DATABASE Neo4j_GDS;

-- Next, we want to consider the warehouse that the GDS application will use to execute queries.
-- For this example, we use a MEDIUM-size warehouse, so we configure the application's warehouse accordingly
ALTER WAREHOUSE Neo4j_GDS_app_warehouse SET WAREHOUSE_SIZE='MEDIUM';
GRANT USAGE ON WAREHOUSE Neo4j_GDS_app_warehouse TO APPLICATION Neo4j_GDS;
-- A highly performant warehouse can speed up graph projections but does not affect algorithm computation.
-- Especially if the views are more complex than shown in this example, a more performant warehouse is beneficial.
-- The warehouse can then be brought back to a less expensive configuration after the projection is done.
-- ALTER WAREHOUSE Neo4j_GDS_app_warehouse WAREHOUSE_SIZE='X-SMALL';

-- The following grants are necessary for the GDS application to read and write data.
-- The next queries are required to read from our prepared views.
GRANT USAGE ON DATABASE             product_recommendation     TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA               product_recommendation.gds TO APPLICATION Neo4j_GDS;
GRANT SELECT ON ALL VIEWS IN SCHEMA product_recommendation.gds TO APPLICATION Neo4j_GDS;
-- This grant is necessary to enable write back of algorithm results.
GRANT CREATE TABLE ON SCHEMA        product_recommendation.gds TO APPLICATION Neo4j_GDS;

-- We have now prepared the environment to properly run the GDS application and can start with our analysis.
-- Note that data preparation and application setup only need to be done once.

-- Our final preparation is to select a compute pool to run the GDS service.
-- Available compute pools to select from are:
-- * CPU_X64_XS
-- * CPU_X64_M
-- * CPU_X64_L
-- * HIGHMEM_X64_S
-- * HIGHMEM_X64_M
-- * HIGHMEM_X64_L
-- * GPU_NV_S - available for GPU-required algorithms only

-- For our example, we use a large compute pool as the node similarity algorithm is computationally intensive, but without extra memory because the graph is quite small.
-- We select: CPU_X64_L


-- ==================================================
-- 3. Graph analysis
-- ==================================================

-- This single procedure call runs the node similarity pipeline end-to-end using the `Neo4j_GDS.graph.node_similarity` procedure.
-- It includes graph projection, computation of similarity, and writing back the results.
CALL Neo4j_GDS.graph.node_similarity('CPU_X64_L', {
    -- The 'project' section defines how to build the in-memory graph.
    -- The defaultTablePrefix simplifies table naming, and node/relationship tables are specified here.
    'project': {
        'defaultTablePrefix': 'product_recommendation.gds',
        -- Tables 'parts' and 'orders' are used as node tables, table name will be treated as a node label.
        'nodeTables': ['parts', 'orders'],
        'relationshipTables': {
            -- The 'part_in_order' table is used as a relationship table, and the sourceNodeId and targetNodeId
            -- columns are specified and contain the node IDs from the correspond node tables.
            -- The relationship table name will be treated as a relationship type.
            'part_in_order': {
                'sourceTable': 'parts',
                'targetTable': 'orders',
                'orientation':  'NATURAL'
            }
        }
    },
    -- The 'compute' section sets algorithm-specific and performance parameters like concurrency for the algorithm.
    'compute': { 'concurrency': 28 },
    -- The 'write' section defines how and where to persist the results of the similarity computation.
    -- It writes the resulting relationships (similarity scores) between parts to a Snowflake table.
    'write': [{
        'sourceLabel': 'parts', 'targetLabel': 'orders', 'outputTable': 'product_recommendation.gds.part_similar_to_part'
    }]
});

-- After writing the table, we need to ensure that our current role is allowed to read it.
-- Alternatively, we can also grant access to all future tables created by the application.
GRANT SELECT ON product_recommendation.gds.part_similar_to_part TO ROLE <your_role>;

-- Since the results are now stored in Snowflake, we can query them and join them with our original data.
-- For example, we can find the names of the most similar parts based on the similarity score.
-- Simply speaking, this could be used as a recommendation system for parts.
SELECT DISTINCT
    p_source.p_name,
    p_target.p_name,
    sim.similarity
FROM product_recommendation.gds.part_similar_to_part sim
    JOIN snowflake_sample_data.tpch_sf1.part p_source
        ON sim.sourcenodeid = p_source.p_partkey
    JOIN snowflake_sample_data.tpch_sf1.part p_target
        ON sim.targetnodeid = p_target.p_partkey
ORDER BY sim.similarity DESC
    LIMIT 10;
