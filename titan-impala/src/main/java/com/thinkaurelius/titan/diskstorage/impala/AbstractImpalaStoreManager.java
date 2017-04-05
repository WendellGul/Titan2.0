package com.thinkaurelius.titan.diskstorage.impala;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StoreMetaData;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.util.List;
import java.util.Map;

/**
 * Created by Great on 2017/3/31.
 */
public class AbstractImpalaStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    public AbstractImpalaStoreManager(Configuration config) {
        super(config, 0);
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return null;
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clearStorage() throws BackendException {

    }

    @Override
    public StoreFeatures getFeatures() {
        return null;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {

    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return null;
    }

    @Override
    public Deployment getDeployment() {
        return null;
    }
}
