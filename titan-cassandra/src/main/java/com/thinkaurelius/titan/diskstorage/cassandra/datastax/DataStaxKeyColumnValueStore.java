package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.*;;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CqlTransformer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.Partitioner;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;


public class DataStaxKeyColumnValueStore implements KeyColumnValueStore {

    private final Session session;
    private final String columnFamilyName;
    private final RetryPolicy retryPolicy;
    private final DataStaxStoreManager storeManager;
    private final DataStaxGetter entryGetter;
    private final DataStaxEdgeGetter edgeEntryGetter;

    DataStaxKeyColumnValueStore(String columnFamilyName,
                                Session session,
                                DataStaxStoreManager storeManager,
                                RetryPolicy retryPolicy) {
        this.session = session;
        this.columnFamilyName = columnFamilyName;
        this.retryPolicy = retryPolicy;
        this.storeManager = storeManager;

        entryGetter = new DataStaxGetter(storeManager.getMetaDataSchema(columnFamilyName));
        edgeEntryGetter = new DataStaxEdgeGetter(storeManager.getMetaDataSchema(columnFamilyName));

    }

    @Override
    public void close() throws BackendException {
        //Do nothing
    }

    @Override
    public MyEntryList getEdgeSlice(EdgeKeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<Long, MyEntryList> result = getEdgeNamesSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(), MyEntryList.EMPTY_LIST);
    }

    @Override
    public Map<Long, MyEntryList> getEdgeSlice(List<Long> keys, EdgeSliceQuery query, StoreTransaction txh) throws BackendException {
        return getEdgeNamesSlice(keys, query, txh);
    }

    public Map<Long, MyEntryList> getEdgeNamesSlice(long key, EdgeSliceQuery query, StoreTransaction txh) {
        return getEdgeNamesSlice(ImmutableList.of(key), query, txh);
    }

    public Map<Long, MyEntryList> getEdgeNamesSlice(List<Long> keys, EdgeSliceQuery query, StoreTransaction txh) {
        List<Statement> statements =
                CqlTransformer.getNamesSliceQuery(columnFamilyName, keys,
                        query.getSliceStart(), query.getSliceEnd(),
                        query.getLimit(), getTx(txh).getReadConsistencyLevel().getDatastax());
        List<ResultSet> rows = new ArrayList<>();

        for(Statement s: statements) {
            rows.add(session.execute(s));
        }

        Map<Long, MyEntryList> result = new HashMap<>();

        int i = 0;
        for (ResultSet row : rows) {
            result.put(keys.get(i++),
                    CassandraHelper.makeMyEntryList(row, edgeEntryGetter, query.getSliceEnd().getId(), query.getLimit()));
        }

        return result;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getNamesSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(),EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNamesSlice(keys, query, txh);
    }

    public Map<StaticBuffer, EntryList> getNamesSlice(StaticBuffer key,
                                                      SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNamesSlice(ImmutableList.of(key),query,txh);
    }

    public Map<StaticBuffer, EntryList> getNamesSlice(List<StaticBuffer> keys,
                                                      SliceQuery query, StoreTransaction txh) throws BackendException {
        List<Statement> statements =
                CqlTransformer.getNamesSliceQuery(columnFamilyName, keys,
                        query.getSliceStart().asByteBuffer(),
                        query.getSliceEnd().asByteBuffer(),
                        query.getLimit(), getTx(txh).getReadConsistencyLevel().getDatastax());

        List<ResultSet> rows = new ArrayList<>();

        for(Statement s : statements) {
            rows.add(session.execute(s));
        }

        Map<StaticBuffer, EntryList> result = new HashMap<>();

        int i = 0;
        for (ResultSet row : rows) {
            result.put(keys.get(i++),
                    CassandraHelper.makeEntryList(row, entryGetter, query.getSliceEnd(), query.getLimit()));
        }

        return result;
    }


    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        mutateMany(ImmutableMap.of(key, new KCVMutation(additions, deletions)), txh);
    }

    @Override
    public void mutateEdge(long key, List<MyEntry> additions, List<MyEntry> deletions, StoreTransaction txh) throws BackendException {
        Map<Long, KCVEdgeMutation> mutations = ImmutableMap.of(key, new KCVEdgeMutation(additions, deletions));
        storeManager.mutateEdge(ImmutableMap.of(columnFamilyName, mutations), txh);
    }

    private void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(columnFamilyName, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        // this query could only be done when byte-ordering partitioner is used
        // because Cassandra operates on tokens internally which means that even contiguous
        // range of keys (e.g. time slice) with random partitioner could produce disjoint set of tokens
        // returning ambiguous results to the user.
        Partitioner partitioner = storeManager.getPartitioner();
        if (partitioner != Partitioner.BYTEORDER)
            throw new PermanentBackendException("getKeys(KeyRangeQuery could only be used with byte-ordering partitioner.");

        StaticBuffer keyStart = query.getKeyStart(), keyEnd = query.getKeyEnd();

        ByteBuffer sliceStart = query.getSliceStart().asByteBuffer(),
                sliceEnd = query.getSliceEnd().asByteBuffer();

        List<Statement> statements = CqlTransformer.getKeysQuery(columnFamilyName,
                keyStart, keyEnd, sliceStart, sliceEnd, query.getLimit(),
                getTx(txh).getReadConsistencyLevel().getDatastax());

        List<ResultSet> rows = new ArrayList<>();
        for(Statement s: statements) {
            ResultSet rs = session.execute(s);
            if(rs != null && rs.one() != null)
                rows.add(rs);
        }
        Iterator<ResultSet> i =
                Iterators.filter(rows.iterator(), new KeySkipPredicate(query.getKeyEnd().asByteBuffer()));
        return new RowIterator(i, query);
    }

    @Override
    public MyKeyIterator getKeys(EdgeKeyRangeQuery query, StoreTransaction txh) throws BackendException {
        Partitioner partitioner = storeManager.getPartitioner();
        if (partitioner != Partitioner.BYTEORDER)
            throw new PermanentBackendException("getKeys(KeyRangeQuery could only be used with byte-ordering partitioner.");

        long keyStart = query.getKeyStart(), keyEnd = query.getKeyEnd();

        List<Statement> statements = CqlTransformer.getKeysQuery(columnFamilyName,
                keyStart, keyEnd, query.getSliceStart(), query.getSliceEnd(), query.getLimit(),
                getTx(txh).getReadConsistencyLevel().getDatastax());

        List<ResultSet> rows = new ArrayList<>();
        for(Statement s: statements) {
            ResultSet rs = session.execute(s);
            if(rs != null && rs.one() != null)
                rows.add(rs);
        }
        Iterator<ResultSet> i =
                Iterators.filter(rows.iterator(), new MyKeySkipPredicate(query.getKeyEnd()));
        return new MyRowIterator(i, query);
    }

    @Override
    public MyKeyIterator getKeys(EdgeSliceQuery sliceQuery, StoreTransaction txh) throws BackendException {
        if (storeManager.getPartitioner() != Partitioner.RANDOM)
            throw new PermanentBackendException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        Statement statement;

        if (sliceQuery != null) {
            statement = CqlTransformer.getKeysQuery(columnFamilyName,
                    sliceQuery.getSliceStart(),
                    sliceQuery.getSliceEnd(),
                    sliceQuery.getLimit());
        }
        else {
            statement = CqlTransformer.getKeysQuery(columnFamilyName, storeManager.getPageSize());
        }
        statement.setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getDatastax());

        ResultSet result = session.execute(statement);

        return new MyRowIterator(Arrays.asList(new ResultSet[]{result}).iterator(), sliceQuery);
    }

    private static class KeySkipPredicate implements Predicate<ResultSet> {

        private final ByteBuffer skip;

        KeySkipPredicate(ByteBuffer skip) {
            this.skip = skip;
        }

        @Override
        public boolean apply(@Nullable ResultSet rs) {
            return (rs != null) && (rs.one() != null) && (rs.one().getBytes("key").equals(skip));
        }
    }

    private static class MyKeySkipPredicate implements Predicate<ResultSet> {

        private final long skip;

        MyKeySkipPredicate(long skip) {
            this.skip = skip;
        }

        @Override
        public boolean apply(@Nullable ResultSet rs) {
            return (rs != null) && (rs.one() != null) && (rs.one().getLong("key") == skip);
        }
    }

    @Override
    public KeyIterator getKeys(SliceQuery sliceQuery, StoreTransaction txh) throws BackendException {
        if (storeManager.getPartitioner() != Partitioner.RANDOM)
            throw new PermanentBackendException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        Statement statement;

        if (sliceQuery != null) {
            statement = CqlTransformer.getKeysQuery(columnFamilyName,
                    sliceQuery.getSliceStart().asByteBuffer(),
                    sliceQuery.getSliceEnd().asByteBuffer(),
                    sliceQuery.getLimit());
        }
        else {
            statement = CqlTransformer.getKeysQuery(columnFamilyName, storeManager.getPageSize());
        }
        statement.setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getDatastax());

        ResultSet result = session.execute(statement);

        return new RowIterator(Arrays.asList(new ResultSet[]{result}).iterator(), sliceQuery);
    }

    @Override
    public String getName() {
        return columnFamilyName;
    }

    private static class DataStaxEdgeGetter implements MyEntry.GetEntry<Row> {

        private final EntryMetaData[] schema;

        private DataStaxEdgeGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public MyEntryList getEntries(Row element) {
            return getEdgeOrPropertyEntry(element);
        }

        @Override
        public MyEntry getEntry(Row element) {
            return getEdgeOrPropertyEntry(element).get(0);
        }

        private MyEntryList getEdgeOrPropertyEntry(Row r) {
            MyEntryList entries = new MyArrayEntryList();
            long id = r.getLong("column");
            int dir = r.getInt("direction");
            ColumnDefinitions cds = r.getColumnDefinitions();
            List<Object> values = new ArrayList<>();
            List<String> cNames = new ArrayList<>();
            for(int i = 0; i < cds.size(); i++) {
                Object value = r.getObject(i);
                if(value == null)
                    continue;
                if(value instanceof List && ((List) value).isEmpty())
                    continue;
                values.add(value);
                cNames.add(cds.getName(i));
            }
            if(dir == 0) {
                for(int i = 2; i < values.size(); i++) {
                    if(jumpColumnNames(cNames.get(i)))
                        continue;
                    TypeDefinitionCategory typeValue = null;
                    if (checkColumnName(cNames.get(i))) {
                        typeValue = TypeDefinitionCategory.of(cNames.get(i));
                    }
                    Object value = parseValue(values.get(i), typeValue);
                    entries.add(new PropertyEntry(id, value, typeValue));
                }
            }
            else {
                int columnNum = values.size();
                long edgeId = r.getLong("edgeId");
                long otherVertexId = r.getLong("otherId");
                MyEntry entry = new EdgeEntry(edgeId, id, dir, otherVertexId);
                if(columnNum > 5) {
                    int i = 5;
                    while(i < columnNum) {
                        long pId = r.getLong(i);
                        Object pValue = values.get(i);
                        assert pValue != null;
                        TypeDefinitionCategory typeValue = null;
                        if(checkColumnName(cNames.get(i))) {
                            typeValue = TypeDefinitionCategory.of(cNames.get(i));
                        }
                        ((EdgeEntry) entry).addProperty(pId, pValue, typeValue);
                        i++;
                    }
                }
                // todo add sortKeys and signatures

                entries.add(entry);
            }
            return entries;
        }

        @Override
        public Long getColumn(Row element) {
            return element.getLong("column");
        }

        @Override
        public EntryMetaData[] getMetaSchema(Row element) {
            return schema;
        }

        @Override
        public Object getMetaData(Row element, EntryMetaData meta) {
            return null;
        }

        private boolean jumpColumnNames(String name) {
            if(name.equals("direction") || name.equals("101") || name.equals("edgeid"))
                return true;
            return false;
        }

        private boolean checkColumnName(String name) {
            for(TypeDefinitionCategory t : TypeDefinitionCategory.values()) {
                if(t.name().toLowerCase().equals(name.toLowerCase()))
                    return true;
            }
            return false;
        }

        private Object parseValue(Object value, TypeDefinitionCategory typeValue) {
            if(typeValue == null)
                return value;
            try {
                switch (typeValue) {
                    case DATATYPE:
                        return Class.forName((String) value);
                    default:
                        return value;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    private static class DataStaxGetter implements StaticArrayEntry.GetColVal<Row, ByteBuffer> {

        private final EntryMetaData[] schema;

        private DataStaxGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }


        @Override
        public ByteBuffer getColumn(Row element) {
            return element.getBytes("column");
        }

        @Override
        public ByteBuffer getValue(Row element) {
            return element.getBytes("value");
        }

        @Override
        public EntryMetaData[] getMetaSchema(Row element) {
            return schema;
        }

        @Override
        public Object getMetaData(Row element, EntryMetaData meta) {
            return null;
        }
    }

    private static class KeyIterationPredicate implements Predicate<ResultSet> {
        @Override
        public boolean apply(@Nullable ResultSet row) {
            return (row != null) && row.getColumnDefinitions().size() > 0;
        }
    }

    private class RowIterator implements KeyIterator {
        private final Iterator<ResultSet> rows;
        private ResultSet currentRow;
        private final SliceQuery sliceQuery;
        private boolean isClosed;

        RowIterator(Iterator<ResultSet> rows, SliceQuery sliceQuery) {
            this.rows = Iterators.filter(rows, new KeyIterationPredicate());
            this.sliceQuery = sliceQuery;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (sliceQuery == null)
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

            return new RecordIterator<Entry>() {
                private final Iterator<Entry> columns =
                        CassandraHelper.makeEntryIterator(currentRow, entryGetter,
                                sliceQuery.getSliceEnd(), sliceQuery.getLimit());

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return columns.next();
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            currentRow = rows.next();
            return StaticArrayBuffer.of(currentRow.one().getBytes("key"));
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }

    private class MyRowIterator implements MyKeyIterator {
        private final Iterator<ResultSet> rows;
        private ResultSet currentRow;
        private final EdgeSliceQuery sliceQuery;
        private boolean isClosed;
        private Long curLong;

        MyRowIterator(Iterator<ResultSet> rows, EdgeSliceQuery sliceQuery) {
            this.rows = Iterators.filter(rows, new KeyIterationPredicate());
            this.sliceQuery = sliceQuery;
            currentRow = null;
            curLong = null;
        }

        @Override
        public RecordIterator<MyEntry> getEntries() {
            ensureOpen();

            if (sliceQuery == null)
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

            return new RecordIterator<MyEntry>() {
                private final Iterator<MyEntry> columns =
                        CassandraHelper.makeEntryIterator(currentRow, edgeEntryGetter,
                                sliceQuery.getSliceEnd().getId(), sliceQuery.getLimit());

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public MyEntry next() {
                    ensureOpen();
                    return columns.next();
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            if(currentRow == null || curLong == null)
                return rows.hasNext();
            Row r = currentRow.one();
            if(r == null)
                return false;
            curLong = r.getLong("key");
            return true;
        }

        @Override
        public Long next() {
            ensureOpen();
            if(currentRow == null) {
                currentRow = rows.next();
                curLong = currentRow.one().getLong("key");
            }
            return curLong;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }

}
