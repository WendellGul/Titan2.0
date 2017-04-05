package com.thinkaurelius.titan.diskstorage.impala.jdbc;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.impala.AbstractImpalaStoreManager;

/**
 * Created by Great on 2017/3/31.
 */
public class JdbcStoreManager extends AbstractImpalaStoreManager {

    public JdbcStoreManager(Configuration config) {
        super(config);
    }

}
