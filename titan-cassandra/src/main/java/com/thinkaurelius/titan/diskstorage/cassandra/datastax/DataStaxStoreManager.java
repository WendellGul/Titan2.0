package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVEdgeMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

@PreInitializeConfigOptions
public class DataStaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(DataStaxStoreManager.class);

    //################### DATASTAX SPECIFIC CONFIGURATION OPTIONS ######################

    public static final ConfigNamespace DATASTAX_NS =
            new ConfigNamespace(CASSANDRA_NS, "astyanax", "Astyanax-specific Cassandra options");

    /**
     * Default name for the Cassandra cluster
     * <p/>
     */
    public static final ConfigOption<String> CLUSTER_NAME =
            new ConfigOption<String>(DATASTAX_NS, "cluster-name",
            "Default name for the Cassandra cluster",
            ConfigOption.Type.MASKABLE, "Titan Cluster");

    /**
     * Maximum pooled connections per host.
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_CONNECTIONS_PER_HOST =
            new ConfigOption<Integer>(DATASTAX_NS, "max-connections-per-host",
            "Maximum pooled connections per host",
            ConfigOption.Type.MASKABLE, 32);

    /**
     * Maximum open connections allowed in the pool (counting all hosts).
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_CONNECTIONS =
            new ConfigOption<Integer>(DATASTAX_NS, "max-connections",
            "Maximum open connections allowed in the pool (counting all hosts)",
            ConfigOption.Type.MASKABLE, -1);

    /**
     * Maximum number of operations allowed per connection before the connection is closed.
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_OPERATIONS_PER_CONNECTION =
            new ConfigOption<Integer>(DATASTAX_NS, "max-operations-per-connection",
            "Maximum number of operations allowed per connection before the connection is closed",
            ConfigOption.Type.MASKABLE, 100 * 1000);

    /**
     * Maximum pooled "cluster" connections per host.
     * <p/>
     * These connections are mostly idle and only used for DDL operations
     * (like creating keyspaces).  Titan doesn't need many of these connections
     * in ordinary operation.
     */
    public static final ConfigOption<Integer> MAX_CLUSTER_CONNECTIONS_PER_HOST =
            new ConfigOption<Integer>(DATASTAX_NS, "max-cluster-connections-per-host",
            "Maximum pooled \"cluster\" connections per host",
            ConfigOption.Type.MASKABLE, 3);

    /**
     * How Astyanax discovers Cassandra cluster nodes. This must be one of the
     * values of the Astyanax NodeDiscoveryType enum.
     * <p/>
     */
    public static final ConfigOption<String> NODE_DISCOVERY_TYPE =
            new ConfigOption<String>(DATASTAX_NS, "node-discovery-type",
            "How Astyanax discovers Cassandra cluster nodes",
            ConfigOption.Type.MASKABLE, "RING_DESCRIBE");


    /**
     * In Astyanax, RetryPolicy and RetryBackoffStrategy sound and look similar
     * but are used for distinct purposes. RetryPolicy is for retrying failed
     * operations. RetryBackoffStrategy is for retrying attempts to talk to
     * uncommunicative hosts. This config option controls RetryPolicy.
     */
    public static final ConfigOption<String> RETRY_POLICY =
            new ConfigOption<String>(DATASTAX_NS, "retry-policy",
            "Astyanax's retry policy implementation with configuration parameters",
            ConfigOption.Type.MASKABLE, "com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8");

    /**
     * If non-null, this must be the fully-qualified classname (i.e. the
     * complete package name, a dot, and then the class name) of an
     * implementation of Astyanax's RetryBackoffStrategy interface. This string
     * may be followed by a sequence of integers, separated from the full
     * classname and from each other by commas; in this case, the integers are
     * cast to native Java ints and passed to the class constructor as
     * arguments. Here's an example setting that would instantiate an Astyanax
     * FixedRetryBackoffStrategy with an delay interval of 1s and suspend time
     * of 5s:
     * <p/>
     * <code>
     * com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000
     * </code>
     * <p/>
     * If null, then Astyanax uses its default strategy, which is an
     * ExponentialRetryBackoffStrategy instance. The instance parameters take
     * Astyanax's built-in default values, which can be overridden via the
     * following config keys:
     * <p/>
     * In Astyanax, RetryPolicy and RetryBackoffStrategy sound and look similar
     * but are used for distinct purposes. RetryPolicy is for retrying failed
     * operations. RetryBackoffStrategy is for retrying attempts to talk to
     * uncommunicative hosts. This config option controls RetryBackoffStrategy.
     */
    public static final ConfigOption<String> RETRY_BACKOFF_STRATEGY =
            new ConfigOption<String>(DATASTAX_NS, "retry-backoff-strategy",
            "Astyanax's retry backoff strategy with configuration parameters",
            ConfigOption.Type.MASKABLE, "com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000");


    /**
     * Controls the frame size of thrift sockets created by Astyanax.
     */
    public static final ConfigOption<Integer> THRIFT_FRAME_SIZE =
            new ConfigOption<Integer>(DATASTAX_NS, "frame-size",
            "The thrift frame size in mega bytes", ConfigOption.Type.MASKABLE, 15);

    public static final ConfigOption<String> LOCAL_DATACENTER =
            new ConfigOption<String>(DATASTAX_NS, "local-datacenter",
            "The name of the local or closest Cassandra datacenter.  When set and not whitespace, " +
            "this value will be passed into ConnectionPoolConfigurationImpl.setLocalDatacenter. " +
            "When unset or set to whitespace, setLocalDatacenter will not be invoked.",
            /* It's between either LOCAL or MASKABLE.  MASKABLE could be useful for cases where
               all the Titan instances are closest to the same Cassandra DC. */
            ConfigOption.Type.MASKABLE, String.class);

    private final String DRIVER_CLASS = "org.apache.cassandra.cql.jdbc.CassandraDriver";

    private final String URL_PREFIX = "jdbc:cassandra://";

    private final Connection connection;

    private final String clusterName;

    private final Cluster cluster;

    private final Session session;

    private final String localDatacenter;

    private final Map<String, DataStaxKeyColumnValueStore> openStores;

    private final Set<String> columns;

    private String columnName;

    public DataStaxStoreManager(Configuration config) throws Exception {
        super(config);

        this.clusterName = config.get(CLUSTER_NAME);

        localDatacenter = config.has(LOCAL_DATACENTER) ?
                config.get(LOCAL_DATACENTER) : "";

        final int maxConnsPerHost = config.get(MAX_CONNECTIONS_PER_HOST);

        final int maxClusterConnsPerHost = config.get(MAX_CLUSTER_CONNECTIONS_PER_HOST);

        this.cluster = Cluster.builder()
                .withClusterName(clusterName)
                .addContactPoints(hostnames)
                .withPort(port)
                .build();

        ensureKeyspaceExists();

        this.session = cluster.connect(keySpaceName);

        Class.forName(DRIVER_CLASS);
        connection = DriverManager.getConnection(getURL());

        openStores = new HashMap<String, DataStaxKeyColumnValueStore>(8);
        columns = new HashSet<>();

        columnName = null;
    }

    private String getURL() {
        if(hostnames.length > 1) {
            // todo
            return null;
        }
        else {
            return URL_PREFIX + hostnames[0] + "/" + keySpaceName;
        }
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE; // TODO
    }

    @Override
    @SuppressWarnings("unchecked")
    public IPartitioner getCassandraPartitioner() throws BackendException {
        try {
            return FBUtilities.newPartitioner(cluster.getMetadata().getPartitioner());
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        } catch (ConfigurationException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String toString() {
        return "datastax" + super.toString();
    }

    @Override
    public void close() {
        openStores.clear();
        session.close();
        cluster.close();
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized DataStaxKeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            ensureColumnFamilyExists(name);
            DataStaxKeyColumnValueStore store = new DataStaxKeyColumnValueStore(name, session, this, null);
            openStores.put(name, store);
            return store;
        }
    }

    private void ensureKeyspaceExists() throws BackendException {
        try {
            KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keySpaceName);
            if(keyspaceMetadata != null && keyspaceMetadata.getName().equals(keySpaceName)) {
                log.debug("Found keyspace {}", keySpaceName);
                return;
            }
        } catch (ConnectionException e) {
            log.debug("Failed to describe keyspace {}", keySpaceName);
        }

        log.debug("Creating keyspace {}...", keySpaceName);
        Session session = null;
        try {
            session = cluster.connect();
            Map<String, Object> replication = new HashMap<>();
            replication.put("class", "SimpleStrategy");
            replication.put("replication_factor", 1);
            com.datastax.driver.core.Statement statement = SchemaBuilder.createKeyspace(keySpaceName)
                    .with()
                    .replication(replication);
            session.execute(statement);
            log.debug("Created keyspace {}", keySpaceName);
        } catch (ConnectionException e) {
            log.debug("Failed to create keyspace {}", keySpaceName);
            throw new TemporaryBackendException(e);
        } finally {
            if(session != null)
                session.close();
        }
    }

    private void ensureColumnFamilyExists(String name) throws BackendException {
        try {
            KeyspaceMetadata dsMeta = cluster.getMetadata().getKeyspace(keySpaceName);
            boolean found = false;
            if (null != dsMeta) {
                for (TableMetadata tabMeta : dsMeta.getTables()) {
                    found |= tabMeta.getName().equals(name);
                }
            }
            if (!found) {
                Statement statement = null;
                if(name.equals(Backend.EDGESTORE_NAME)) {
                    statement = SchemaBuilder.createTable(name)
                            .addPartitionKey("key", DataType.bigint())
                            .addClusteringColumn("column", DataType.bigint())
                            .addClusteringColumn("edgeId", DataType.bigint())
                            .addColumn("direction", DataType.cint())
                            .addColumn("otherId", DataType.bigint());
                }
                else {
                    statement = SchemaBuilder.createTable(name)
                            .addPartitionKey("key", DataType.blob())
                            .addClusteringColumn("column", DataType.blob())
                            .addColumn("value", DataType.blob());
                }
                session.execute(statement);
            }
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {

            // Not a big deal if Keyspace doesn't not exist (dropped manually by user or tests).
            // This is called on per test setup basis to make sure that previous test cleaned
            // everything up, so first invocation would always fail as Keyspace doesn't yet exist.
            if(cluster.getMetadata().getKeyspace(keySpaceName) == null)
                return;

            KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keySpaceName);

            for(TableMetadata tm : keyspaceMetadata.getTables()) {
                session.execute(QueryBuilder.truncate(tm));
                if(tm.getName().equals(Backend.EDGESTORE_NAME)) {
                    List<Statement> statements = new ArrayList<>();
                    for(ColumnMetadata cm : tm.getColumns()) {
                        if(isNameOriginal(cm.getName()))
                            continue;
                        statements.add(SchemaBuilder.alterTable(Backend.EDGESTORE_NAME)
                                .dropColumn("\"" + cm.getName() + "\""));
                    }
                    if(!statements.isEmpty()) {
                        for (Statement s: statements)
                            session.execute(s);
                    }
                }
            }

        } catch (ConnectionException e) {
            throw new PermanentBackendException(e);
        }
    }

    private boolean isNameOriginal(String name) {
        if(name.equals("key") || name.equals("column")
                || name.toLowerCase().equals("edgeid") || name.toLowerCase().equals("direction")
                || name.toLowerCase().equals("otherid"))
            return true;
        return false;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> batch, StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        BatchStatement bs = new BatchStatement();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchentry : batch.entrySet()) {
            String storeName = batchentry.getKey();
            Preconditions.checkArgument(openStores.containsKey(storeName), "Store cannot be found: " + storeName);

            String columnFamilyName = openStores.get(storeName).getName();

            Map<StaticBuffer, KCVMutation> mutations = batchentry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                // The CLMs for additions and deletions are separated because
                // Astyanax's operation timestamp cannot be set on a per-delete
                // or per-addition basis.
                KCVMutation titanMutation = ent.getValue();
                ByteBuffer key = ent.getKey().asByteBuffer();

                if (titanMutation.hasDeletions()) {
                    for (StaticBuffer b : titanMutation.getDeletions())
                        bs.add(QueryBuilder.delete().from(columnFamilyName)
                                .where(QueryBuilder.eq("key", b.asByteBuffer()))
                                .using(QueryBuilder.timestamp(commitTime.getDeletionTime(times))));
                }

                if (titanMutation.hasAdditions()) {
                    for (Entry e : titanMutation.getAdditions()) {
                        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);

                        Insert insert = QueryBuilder.insertInto(columnFamilyName)
                                .value("key", key)
                                .value("column", e.getColumnAs(StaticBuffer.BB_FACTORY))
                                .value("value", e.getValueAs(StaticBuffer.BB_FACTORY));

                        if (null != ttl && ttl > 0) {
                            insert.using(QueryBuilder.ttl(ttl));
                        }
                        insert.using(QueryBuilder.timestamp(commitTime.getAdditionTime(times)));
                        bs.add(insert);
                    }
                }
            }
        }

        try {
            session.execute(bs);
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        }

        sleepAfterWrite(txh, commitTime);
    }

    public void mutateEdge(Map<String, Map<Long, KCVEdgeMutation>> batch, StoreTransaction txh) throws BackendException {

        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        BatchStatement alterBs = new BatchStatement(), insertBs = new BatchStatement();

        for(Map.Entry<String, Map<Long, KCVEdgeMutation>> batchentry : batch.entrySet()) {
            String storeName = batchentry.getKey();
            Preconditions.checkArgument(openStores.containsKey(storeName), "Store cannot be found: " + storeName);

            String columnFamilyName = openStores.get(storeName).getName();

            Map<Long, KCVEdgeMutation> mutations = batchentry.getValue();

            for(Map.Entry<Long, KCVEdgeMutation> ent : mutations.entrySet()) {
                KCVEdgeMutation titanMutation = ent.getValue();
                long key = ent.getKey();

                if(titanMutation.hasDeletions()) {
                    for(MyEntry e : titanMutation.getDeletions()) {
                        insertBs.add(QueryBuilder.delete().from(columnFamilyName)
                                .where(QueryBuilder.eq("key", key))
                                .and(QueryBuilder.eq("column", e.getColumn()))
                                .and(QueryBuilder.eq("edgeid", e.getEdgeId()))
                                .using(QueryBuilder.timestamp(commitTime.getDeletionTime(times))));
                    }
                }

                if(titanMutation.hasAdditions()) {
                    for(MyEntry e : titanMutation.getAdditions()) {
                        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);

                        List<Statement> alterStatements = null;
                        Insert insert = null;

                        if(e instanceof PropertyEntry) {
                            alterStatements = Collections.singletonList(addPropertyColumn(columnFamilyName, (PropertyEntry) e));
                            Object propertyValue = ((PropertyEntry) e).getPropertyValue();
                            insert = QueryBuilder.insertInto(columnFamilyName)
                                    .value("key", key)
                                    .value("column", e.getColumn())
                                    .value("edgeid", e.getEdgeId())
                                    .value("direction", e.getDirectionId())
                                    .value(this.columnName, CassandraHelper.checkValue(propertyValue));
                        }
                        else {
                            List<String> names = new ArrayList<>();
                            List<Object> values = new ArrayList<>();

                            alterStatements = addPropertyColumns(columnFamilyName, (EdgeEntry) e);
                            getNamesAndValues(names, values, key, (EdgeEntry) e);

                            insert = QueryBuilder.insertInto(columnFamilyName).values(names, values);
                        }

                        if(null != ttl && ttl > 0) {
                            insert.using(QueryBuilder.ttl(ttl));
                        }
                        insert.using(QueryBuilder.timestamp(commitTime.getAdditionTime(times)));

                        if(!alterStatements.isEmpty()) {
                            for (Statement s : alterStatements) {
                                if(s != null) alterBs.add(s);
                            }
                        }
                        insertBs.add(insert);
                    }
                }
            }
        }

        try {
            for(Statement s : alterBs.getStatements())
                session.execute(s);
            session.execute(insertBs);
            alterBs.clear();
            insertBs.clear();
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        }

        sleepAfterWrite(txh, commitTime);
    }

    private List<Statement> addPropertyColumns(String columnFamilyName, EdgeEntry entry) {
        return addPropertyColumns(columnFamilyName, entry.getProperties());
    }

    private List<Statement> addPropertyColumns(String columnFamilyName, List<PropertyEntry> properties) {
        List<Statement> statements = new ArrayList<>();
        for(PropertyEntry p : properties) {
            statements.add(addPropertyColumn(columnFamilyName, p));
        }
        return statements;
    }

    private Statement addPropertyColumn(String columnFamilyName, PropertyEntry p) {
        if(p.getColumn() == BaseKey.SchemaDefinitionProperty.longId()) {
            columnName = p.getPropertyTypeValue().name();
        }
        else
            columnName = "\"" + String.valueOf(p.getColumn()) + "\"";
        if(columns.contains(columnName))
            return null;
        columns.add(columnName);
        return SchemaBuilder.alterTable(columnFamilyName)
                .addColumn(columnName)
                .type(CassandraHelper.getDataType(p.getPropertyValue().getClass()));
    }

    private void getNamesAndValues(List<String> names, List<Object> values, long key, EdgeEntry e) {
        names.add("key");
        values.add(key);
        names.add("column");
        values.add(e.getColumn());
        names.add("edgeId");
        values.add(e.getEdgeId());
        names.add("direction");
        values.add(e.getDirectionId());
        names.add("otherId");
        values.add(e.getOtherVertexId());

        for(PropertyEntry p : e.getProperties()) {
            names.add(String.valueOf(p.getColumn()));
            values.add(p.getPropertyValue());
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws BackendException {
        try {
            if(cluster.getMetadata().getKeyspace(keySpaceName) == null) {
                throw new PermanentBackendException("Keyspace " + keySpaceName + " is undefined");
            }

            KeyspaceMetadata kMeta = cluster.getMetadata().getKeyspace(keySpaceName);

            TableMetadata tabMeta = kMeta.getTable(cf);

            if (null == tabMeta) {
                throw new PermanentBackendException("Column family " + cf + " is undefined");
            }

            return tabMeta.getOptions().getCompression();
        } catch (ConnectionException e) {
            throw new PermanentBackendException(e);
        }
    }
}


