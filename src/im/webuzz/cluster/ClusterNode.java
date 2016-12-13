package im.webuzz.cluster;


/**
 * Cluster node which will be used to provide service. Any server instance
 * or thread implementing this interface can join the cluster. And cluster
 * server will send cluster event and notify duplicated login to this node. 
 * 
 * @author zhourenjian
 *
 */
public interface ClusterNode {

	/**
	 * Get cluster node id. Node ID is server IP and its port. For port 80, it is optional.
	 * Format: <Server IP>[:<Server Port>]
	 * @return
	 */
	public String getNodeID();
	
	/**
	 * Get online users in this cluster node.
	 * 
	 * @return online user IDs
	 */
	public int[] getOnlineUsers();
	
	/**
	 * Cluster node need to pipe given cluster event to the given target.
	 * 
	 * @param event
	 * @return Whether event is sent to target user or not. For user who is
	 * not online, message will be discarded.
	 */
	public boolean onReceivedEvent(ClusterEvent event);
	
	/**
	 * Got user login, maybe from other server. Cluster node should
	 * clean given user's status on this node, if it's duplicated.
	 * 
	 * @param uid
	 * @param time
	 * @param host
	 * @param port
	 */
	public boolean onLoginedElsewhere(int uid, long time, String host, int port);
	
}
