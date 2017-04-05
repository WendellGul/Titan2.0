package com.thinkaurelius.titan.diskstorage.impala.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.util.List;
import java.util.Map;

/**
 * Created by Great on 2017/3/31.
 */
public class JdbcKeyColumnValueStore implements KeyColumnValueStore {

    private final String keyspace;
    private final String columnFamilyName;
    private final JdbcStoreManager storeManager;

    public JdbcKeyColumnValueStore(String keyspace,
                                   String columnFamilyName,
                                   JdbcStoreManager storeManager) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;
        this.storeManager = storeManager;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getNameSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNameSlice(keys, query, txh);
    }

    private Map<StaticBuffer, EntryList> getNameSlice(StaticBuffer key,
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws BackendException {
        return getNameSlice(ImmutableList.of(key), query, txh);
    }

    private Map<StaticBuffer, EntryList> getNameSlice(List<StaticBuffer> keys,
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {

    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {

    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public String getName() {
        return columnFamilyName;
    }

    @Override
    public void close() throws BackendException {

    }
}
