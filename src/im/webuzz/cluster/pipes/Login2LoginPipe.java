package im.webuzz.cluster.pipes;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.events.ServerLost;
import im.webuzz.cluster.events.ServerStatus;
import im.webuzz.cluster.events.ServerStopping;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.xyz.XYZMixxer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.j2s.ajax.annotation.SimpleIn;
import net.sf.j2s.ajax.annotation.SimpleOut;

/**
 * These pipes will be managed at login server.
 * 
 * Slave login server will create and connect a pipe to master login server
 * (primary server). Master login server will pipe server status and user
 * status to slave login server.
 * 
 * Direction:
 * down: login server to service server.
 * up: service server to login server.
 * to: service server to service server
 * 
 * @author zhourenjian
 *
 */
public class Login2LoginPipe extends AbstractClusterPipe {

	private static Object syncDataMutex = new Object();
	
	public static Map<String, Login2LoginPipe> allSyncPipes = new ConcurrentHashMap<String, Login2LoginPipe>();

	private static String[] mappings = new String[] {
			"pipeKey", "k", // Fixed
			"pipeAlive", "a", // Fixed
			"apiSecret", "s",
			"port", "p",
			"domain", "d",
			"onlineServers", "o",
			"target", "t",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	/**
	 * Remote HTTP server port.
	 */
	@SimpleIn
	public int port;

	/**
	 * If not given (early version), remote IP of this RPC is used.
	 */
	@SimpleIn
	public String domain;
	
	@SimpleOut
	public ServerStatus[] onlineServers;
	
	/**
	 * Specify which cluster server IP this RPC is targeting.
	 */
	@SimpleIn
	public String target;
	
	private String host;
	
	// client side
	private boolean stopping;

	// client side
	private boolean synchronizing;
	
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

	protected String getRemoteDomain() {
		if (domain == null) {
			return getRemoteIP();
		}
		return domain;
	}
	
	public String getHost() {
		return host;
	}

	public boolean isStopping() {
		return stopping;
	}

	public void setStopping(boolean stopping) {
		this.stopping = stopping;
	}

	public boolean isSynchronizing() {
		return synchronizing;
	}

	public void setSynchronizing(boolean synchronizing) {
		this.synchronizing = synchronizing;
	}

	@Override
	public boolean pipeSetup() {
		if (!super.pipeSetup()) {
			return false;
		}

		host = port == 80 ? getRemoteDomain() : (getRemoteDomain() + ":" + port);

		Login2LoginPipe p = null;
		synchronized (syncDataMutex) {
			p = allSyncPipes.put(host, this);
			if (p != null) {
				this.pipeCloneData(p, HeartBeatFilter.singleton, true);
			}
		}
		if (p != null) {
			p.pipeClosed();
			removeClusterPipeFromMonitor4Server(p);
			//p = null;
		}
		
		Map<String, ServerStatus> servers = XYZMixxer.getAllServiceServers(host, true);
		onlineServers = servers.values().toArray(new ServerStatus[servers.size()]);
		
		// p == null is always true ? comment out above p = null line in if (p != null) block
		boolean needSynchronizing = (p == null && XYZMixxer.isPrimaryMasterMode()
				&& ClusterConfig.clusterPrimarySynchronizing
				&& XYZMixxer.isJustInitialized());

		String targetDomain = target;
		if (targetDomain != null && !targetDomain.equals(ClusterConfig.clusterServerIP)) {
			String[] allDomains = ClusterConfig.clusterAllServerIPs;
			if (allDomains != null && allDomains.length > 0) {
				boolean existed = false;
				for (int i = 0; i < allDomains.length; i++) {
					if (targetDomain.equals(allDomains[i])) {
						existed = true;
						break;
					}
				}
				if (!existed) {
					if (ClusterConfig.clusterLogging) {
						System.out.println("Cluster: Primary to primary RPC's target domain " + targetDomain + " is not in server IP list.");
					}
					targetDomain = ClusterConfig.clusterServerIP;
				}
			}
		}
		if (targetDomain == null) {
			targetDomain = ClusterConfig.clusterServerIP;
		}
		XYZMixxer.createLogin2LoginPipe(targetDomain, getRemoteDomain(), port, needSynchronizing, true);
		
		// The following XYZMixxer.synchronizeSlots will be invoke on Login2LoginPipe#pipeCreated
		// Only after Login2LoginPipe is created then start synchronizing, or user status may be
		// inaccurate if users update statuses in between slot already be synchronized and pipe not
		// be created.
		/*
		if (needSynchronizing) {
			// Try to synchronize from other login server on restarting
			XYZMixxer.synchronizeSlots(getRemoteDomain(), port, true, 0);
		} // else already existed
		// */
		
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

	public boolean deal(UserResult usr) {
		return true;
	}

	public boolean deal(ServerStatus ss) {
		return true;
	}
	
	public boolean deal(ServerLost sl) {
		return true;
	}

}
