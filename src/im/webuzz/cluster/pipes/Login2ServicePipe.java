package im.webuzz.cluster.pipes;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.events.IStatus;
import im.webuzz.cluster.events.ServerStatus;
import im.webuzz.cluster.events.UserDuplicated;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.xyz.XYZMixxer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.j2s.ajax.SimpleSerializable;
import net.sf.j2s.ajax.annotation.SimpleIn;

/**
 * These pipes will be managed at login server.
 * 
 * Service server will create and connect a pipe to login server (primary
 * server). Login server will pipe down server status and user login query
 * result to service server.
 * 
 * Direction:
 * down: login server to service server.
 * up: service server to login server.
 * to: service server to service server
 * 
 * @author zhourenjian
 *
 */
public class Login2ServicePipe extends AbstractClusterPipe {

	private static Object loginDataMutex = new Object();
	
	public static Map<String, Login2ServicePipe> allLoginPipes = new ConcurrentHashMap<String, Login2ServicePipe>();

	private static String[] mappings = new String[] {
			"pipeKey", "k", // Fixed
			"pipeAlive", "a", // Fixed
			"apiSecret", "s",
			"port", "p",
			"domain", "d",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	/**
	 * Service server's HTTP port.
	 */
	@SimpleIn
	public int port;
	
	@SimpleIn
	public String domain;
	
	private String host;
	
	/* For client side only */
	private String remoteHost;

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

	public String getRemoteDomain() {
		if (domain == null) {
			return getRemoteIP();
		}
		return domain;
	}
	
	public String getHost() {
		return host;
	}
	
	/* For client side only */
	public String getRemoteHost() {
		return remoteHost;
	}

	/* For client side only */
	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}
	
	@Override
	public boolean pipeSetup() {
		if (!super.pipeSetup()) {
			return false;
		}
		setLastHeartbeat(System.currentTimeMillis());
		
		host = port == 80 ? getRemoteDomain() : (getRemoteDomain() + ":" + port);
		Map<String, ServerStatus> onlineServers = new HashMap<String, ServerStatus>(allLoginPipes.size());
		
		if (!ClusterConfig.clusterPrimaryLoginServer) {
			ServerStatus ps = new ServerStatus();
			ps.domain = ClusterConfig.clusterServerIP;
			ps.port = ClusterConfig.port;
			ps.status = IStatus.CONNECTED;
			onlineServers.put(host, ps);
			
			XYZMixxer.createService2ServicePipe(getRemoteDomain(), port);
		}

		Login2ServicePipe p = null;
		
		ServerStatus currentServer = new ServerStatus();
		currentServer.domain = getRemoteDomain();
		currentServer.port = port;
		currentServer.status = IStatus.CONNECTED;
		for (Iterator<Login2ServicePipe> itr = allLoginPipes.values().iterator(); itr.hasNext();) {
			Login2ServicePipe pipe = itr.next();
			if (!host.equals(pipe.host)) {
				ServerStatus ss = new ServerStatus();
				ss.domain = pipe.getRemoteDomain();
				ss.port = pipe.port;
				if (Math.abs(System.currentTimeMillis() - pipe.getLastHeartbeat()) > ClusterConfig.clusterPipeHeartBeatTimeoutInterval) {
					// Server is down! Ignore this server
					ss.status = IStatus.DISCONNECTED;
					onlineServers.put(pipe.host, ss);
					continue;
				}
				ss.status = IStatus.CONNECTED;
				onlineServers.put(pipe.host, ss);
				// pipe this pipe's another end (service server) to all existed
				// service server
				pipe.pipeThrough(currentServer);
			}
		}
		synchronized (loginDataMutex) {
			p = allLoginPipes.put(host, this);
			if (p != null) {
				this.pipeCloneData(p, HeartBeatFilter.singleton, true);
			}
		}
		
		if (!XYZMixxer.isPrimaryProxyMode()) {
			// service servers on other primary login servers
			for (Iterator<Entry<String, ServerStatus>> itr = XYZMixxer.allServerStatuses.entrySet().iterator(); itr.hasNext();) {
				Entry<String, ServerStatus> entry = (Entry<String, ServerStatus>) itr.next();
				String remoteHost = entry.getKey();
				if (host.equals(remoteHost) || onlineServers.containsKey(remoteHost)) {
					continue;
				}
				onlineServers.put(remoteHost, entry.getValue());
			}
		} // Proxy server won't expose services on other primary servers to inner service servers

		int size = onlineServers.size();
		if (size > 0) {
			// pipe all existed service servers down to this pipe's another end
			pipeThrough(onlineServers.values().toArray(new SimpleSerializable[size]));
		}
		
		if (ClusterConfig.clusterPrimarySynchronizing && !XYZMixxer.isPrimaryProxyMode()) {
			// Pipe current server to all slave login servers
			for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
					itr.hasNext();) {
				Login2LoginPipe l2lPipe = (Login2LoginPipe) itr.next();
				l2lPipe.pipeThrough(currentServer);
			}
		} // Primary proxy server won't expose inner service servers to outer world

		if (p != null) {
			p.pipeClosed();
			removeClusterPipeFromMonitor4Server(p);
			p = null;
		}
		
		// create and connect Service2Login pipe from this login server to
		// service server.
		XYZMixxer.createService2LoginPipe(getRemoteDomain(), port);
		
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
	
	/**
	 * Service server may create or destroy a new Service2ServicePipe to
	 * another service server, on receiving this event.
	 * 
	 * @param ss
	 * @return
	 */
	public boolean deal(ServerStatus ss) {
		return true;
	}

	/**
	 * Service server gets user server query result from login server. Service
	 * server may try to send cached cluster events to the server specified in
	 * this event.
	 * 
	 * @param usr
	 * @return
	 */
	public boolean deal(UserResult usr) {
		return true;
	}

	/**
	 * Service server gets user duplicated events from login server. Service
	 * server should try to notify client that user is login from another
	 * server.
	 * 
	 * @param usd
	 * @return
	 */
	public boolean deal(UserDuplicated usd) {
		return true;
	}

}
