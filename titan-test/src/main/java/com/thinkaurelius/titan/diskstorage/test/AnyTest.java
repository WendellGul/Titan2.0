package com.thinkaurelius.titan.diskstorage.test;


import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

/**
 * Created by Great on 2017/3/31.
 */
public class AnyTest {
    static final String KEYSPACE_NAME = "ks_test";
    static final String CF_NAME = "tb_test";

    public static void main(String[] args) throws Exception {
        test1();
    }

    static void test2() {
        System.err.printf("%ld", 123);
    }

    static void test1() {
        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withPort(9042)
                .build();
        Session session = cluster.connect(KEYSPACE_NAME);
        Statement statement = SchemaBuilder.createTable("guwen")
                .addPartitionKey("a", DataType.bigint())
                .addClusteringColumn("b", DataType.bigint())
                .addColumn("c", DataType.text());
        session.execute(statement);
        session.close();
        cluster.close();
    }


}
