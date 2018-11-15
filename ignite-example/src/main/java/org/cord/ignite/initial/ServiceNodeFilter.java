package org.cord.ignite.initial;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

public class ServiceNodeFilter implements IgnitePredicate<ClusterNode>{
    /**
     * Checks if {@code node} needs to be considered as a Data Node.
     *
     * @param node Cluster node instance.
     *
     * @return {@code true} if the node has to be considered as Data Node, {@code false} otherwise.
     */
    public boolean apply(ClusterNode node) {
        Boolean dataNode = node.attribute("service.node");
        return dataNode != null && dataNode;
    }
}
