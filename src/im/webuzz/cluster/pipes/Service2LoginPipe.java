package im.webuzz.cluster.pipes;

import java.util.Map;

import net.sf.j2s.ajax.annotation.SimpleOut;

import im.webuzz.cluster.ClusterNode;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.events.ServerStopping;
import im.webuzz.cluster.events.UserQuery;
import im.webuzz.cluster.events.UserStatus;

/**
 * These pipes will be managed on service servers.
 * 
 * Login server will create and connect these pipes to all existed service
 * servers. Service servers will pipe up user login and user query event
 * to login server. 
 * 
 * Service server will try to keep only one Service2LoginPipe.
 * 
 * Direction:
 * down: login server to service server.
 * up: service server to login server.
 * to: service server to service server
 * 
 * @author zhourenjian
 *
 */
public class Service2LoginPipe extends AbstractClusterPipe {

	public static Object pipeMutex = new Object();
	
	public static Service2LoginPipe servicePipe = null;

	private static String[] mappings = new String[] {
			"pipeKey", "k", // Fixed
			"pipeAlive", "a", // Fixed
			"apiSecret", "s",
			"onlineUsers", "o",
			"time", "t",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	@SimpleOut
	public int[] onlineUsers;
	
	@SimpleOut
	public long time;

	private boolean stopping;
	
	@Override
	protected Map<String, String> fieldNameMapping() {
		return nameMappings;
	}
	
	@Override
	protected Map<String, String> fieldAliasMapping() {
		return aliasMappings;
	}
	
	@Override
	protected String[] fieldMapping() {
		return mappings;
	}

	public boolean isStopping() {
		return stopping;
	}

	public void setStopping(boolean stopping) {
		this.stopping = stopping;
	}

	@Override
	public boolean pipeSetup() {
		if (!super.pipeSetup()) {
			return false;
		}
		Service2LoginPipe p = null;
		synchronized (pipeMutex) {
			p = servicePipe;
			servicePipe = this;
			if (p != null) {
				this.pipeCloneData(p, HeartBeatFilter.singleton, true);
			}
		}
		if (p != null) {
			p.pipeClosed();
			removeClusterPipeFromMonitor4Server(p);
			p = null;
		}
		time = System.currentTimeMillis();
		
		ClusterNode serviceNode = ClusterServer.getClusterNode();
		if (serviceNode != null) {
			onlineUsers = serviceNode.getOnlineUsers();
		}
		return true;
	}

	/**
	 * Server is having its normal heart beat.
	 * @param shb
	 * @return
	 */
	public boolean deal(HeartBeat shb) {
		return true;
	}

	public boolean deal(ServerStopping ssp) {
		return true;
	}

	/**
	 * Login server will keep user-server statuses.
	 * 
	 * @param uss
	 * @return
	 */
	public boolean deal(UserStatus uss) {
		return true;
	}

	/**
	 * On receiving user-server queries, login server will search the
	 * server that user is connecting, and pipe down it as a result in
	 * Login2ServicePipe.
	 * 
	 * @param usq
	 * @return
	 */
	public boolean deal(UserQuery usq) {
		return true;
	}

}
