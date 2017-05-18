package com.thinkaurelius.titan.diskstorage.test;

import com.datastax.driver.core.*;

/**
 * Created by Great on 2017/3/31.
 */
public class CassandraJdbcTest {

    private static final String[] HOSTS = {"127.0.0.1"};
    private static final int PORT = 9042;

    private static final String QUERY_CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS temp WITH replication " +
            "= {'class':'SimpleStrategy', 'replication_factor':1};";

    private static final String QUERY_DROP_KEYSPACE = "DROP KEYSPACE temp;";

    private static final String QUERY_GET_ALL_KEYS = "SELECT * FROM tb_test;";

    public static void main(String[] args) {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoints(HOSTS).withPort(PORT)
                    .build();
            getKeyspaceMetaData(cluster);
            Session session = cluster.connect("ks_test");
            getAllKeys(session);
        }
        finally {
            if(cluster != null)
                cluster.close();
        }
    }

    public static void getAllKeys(Session session) {
        ResultSet rs = session.execute(QUERY_GET_ALL_KEYS);
        for(Row r : rs) {
            ColumnDefinitions cd = r.getColumnDefinitions();
            for(int i = 0; i < cd.size(); i++) {
                String name = cd.getName(i);
                DataType.Name typeName = cd.getType(i).getName();
                System.out.print(name + " " + typeName.name() + "\t");
            }
            System.out.println();
        }
    }



    public static void getKeyspaceMetaData(Cluster cluster) {
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s%n", metadata.getClusterName());

        for (KeyspaceMetadata keyspace : metadata.getKeyspaces())
            System.out.printf("Keyspace: %s%n", keyspace.getName());
        System.out.println();
        System.out.println();
    }

    public static String bytesToHexString(byte[] b) {
        StringBuffer sb = new StringBuffer();
        sb.append("0x");
        for(int i = 0; i < b.length; i++) {
            sb.append(Integer.toHexString(b[i]));
        }
        return sb.toString();
    }

}
