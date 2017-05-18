package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class EdgeLabelVertex extends RelationTypeVertex implements EdgeLabel {

    public EdgeLabelVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public boolean isDirected() {
        return isUnidirected(Direction.BOTH);
    }

    @Override
    public boolean isUnidirected() {
        return isUnidirected(Direction.OUT);

    }

    @Override
    public boolean isUnidirected(Direction dir) {
        int dirId = getDefinition().getValue(TypeDefinitionCategory.UNIDIRECTIONAL, Integer.class);
        return IDHandler.DirectionID.forId(dirId).getDirection() == dir;
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }
}
