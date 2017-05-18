package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.MyEntry;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface RelationReader {

    public RelationCache parseRelation(Entry data, boolean parseHeaderOnly, TypeInspector tx);

    public RelationCache parseRelation(MyEntry data, boolean parseHeaderOnly, TypeInspector tx);

}
