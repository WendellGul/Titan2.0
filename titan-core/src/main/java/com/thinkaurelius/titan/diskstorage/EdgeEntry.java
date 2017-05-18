package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Great on 2017/5/14.
 */
public class EdgeEntry implements MyEntry {

    private long edgeId;

    private long labelId;

    private int dir;

    private Map<Long, Object> sortKeys;

    private long otherVertexId;

    private Map<Long, Object> signatures;

    private List<PropertyEntry> properties;

    private volatile transient RelationCache cache;

    public EdgeEntry(long edgeId, long labelId, int dir, long otherVertexId) {
        this.edgeId = edgeId;
        this.labelId = labelId;
        this.dir = dir;
        this.otherVertexId = otherVertexId;
        this.properties = new ArrayList<>();
        this.signatures = new HashMap<>();
        this.sortKeys = new HashMap<>();
    }

    private Map<EntryMetaData, Object> metadata = EntryMetaData.EMPTY_METADATA;

    @Override
    public long getColumn() {
        return labelId;
    }

    @Override
    public boolean isEdge() {
        return true;
    }

    @Override
    public boolean isProperty() {
        return false;
    }

    @Override
    public boolean hasMetaData() {
        return !metadata.isEmpty();
    }

    @Override
    public Map<EntryMetaData, Object> getMetaData() {
        return metadata;
    }

    @Override
    public int compareTo(MyEntry o) {
        assert o instanceof EdgeEntry;
        return Long.compare(edgeId, ((EdgeEntry) o).edgeId);
    }

    @Override
    public Object setMetaData(EntryMetaData key, Object value) {
        if (metadata==EntryMetaData.EMPTY_METADATA) metadata = new EntryMetaData.Map();
        return metadata.put(key,value);
    }

    public void addProperty(long type, Object value, TypeDefinitionCategory typeValue) {
        PropertyEntry property = new PropertyEntry(type, value, typeValue);
        assert !properties.contains(property);
        properties.add(property);
    }

    public void addSortKey(Long key, Object value) {
        assert !sortKeys.containsKey(key);
        sortKeys.put(key, value);
    }

    public void addSignature(Long key, Object value) {
        assert !signatures.containsKey(key);
        signatures.put(key, value);
    }

    public long getEdgeId() {
        return edgeId;
    }

    public long getOtherVertexId() {
        return otherVertexId;
    }

    public int getDirectionId() {
        return dir;
    }

    public List<PropertyEntry> getProperties() {
        return properties;
    }

    public Map<Long, Object> getSortKeys() {
        return sortKeys;
    }

    public Map<Long, Object> getSignatures() {
        return signatures;
    }

    public Object getProperty(long pId) {
        for(PropertyEntry p : properties) {
            if(p.getColumn() == pId)
                return p;
        }
        return null;
    }

    @Override
    public RelationCache getCache() {
        return cache;
    }

    @Override
    public void setCache(RelationCache cache) {
        Preconditions.checkNotNull(cache);
        this.cache = cache;
    }

    @Override
    public boolean hasRemaining() {
        return !properties.isEmpty();
    }
}
