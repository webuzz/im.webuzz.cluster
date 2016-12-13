package im.webuzz.cluster.pipes;

import im.webuzz.cluster.ClusterEvent;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.xyz.XYZMixxer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.j2s.ajax.annotation.SimpleIn;

/**
 * Service server will create and connect this pipe to another service
 * server. Normal user update, message event or login elsewhere event will
 * be piped from one service server to another service server through these
 * pipes. 
 * 
 * Direction:
 * down: login server to service server.
 * up: service server to login server.
 * to: service server to service server
 * 
 * @author zhourenjian
 *
 */
public class Service2ServicePipe extends AbstractClusterPipe {

	private static Object serviceDataMutex = new Object();
	
	public static Map<String, Service2ServicePipe> allServicePipes = new ConcurrentHashMap<String, Service2ServicePipe>();

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
	 * Remote HTTP server port.
	 */
	@SimpleIn
	public int port;
	
	/**
	 * Remote HTTP server host name or IP.
	 */
	@SimpleIn
	public String domain;

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

	@Override
	public boolean pipeSetup() {
		if (!super.pipeSetup()) {
			return false;
		}

		String host = domain;
		if (domain == null || domain.length() == 0) {
			host = getRemoteIP();
		}
		String remoteHost = port == 80 ? host : (host + ":" + port);

		Service2ServicePipe p = null;
		synchronized (serviceDataMutex) { // this mutex is used to clone pipe data synchronously
			p = allServicePipes.put(remoteHost, this);
			if (p != null) {
				this.pipeCloneData(p, HeartBeatFilter.singleton, true);
			}
		}
		if (p != null) {
			p.pipeClosed();
			removeClusterPipeFromMonitor4Server(p);
			p = null;
		}
		XYZMixxer.createService2ServicePipe(host, port);
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

	public boolean deal(ClusterEvent e) {
		return true;
	}

}
