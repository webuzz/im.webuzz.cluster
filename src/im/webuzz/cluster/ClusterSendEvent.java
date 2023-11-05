package im.webuzz.cluster;

import java.util.Map;

import net.sf.j2s.ajax.SimpleRPCRequest;
import net.sf.j2s.ajax.SimpleRPCRunnable;
import net.sf.j2s.ajax.annotation.SimpleIn;
import net.sf.j2s.ajax.annotation.SimpleOut;

import im.webuzz.cluster.xyz.XYZMixxer;

/**
 * Send an event to cluster server.
 * 
 * Usage:
 * <code>
 * ClusterSendEvent r = new ClusterSendEvent() {
 * 	@Override
 * 	public String getHttpURL() {
 * 		return "http://www.domain.com/u/r";
 * 	}
 * };
 * r.events = evts;
 * r.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
 * SimpleRPCRequest.request(r);
 * </code>
 * 
 * @author zhourenjian
 *
 */
public class ClusterSendEvent extends SimpleRPCRunnable {

	private static String[] mappings = new String[] {
			"apiSecret", "a",
			"events", "e",
			"results", "r",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	@SimpleIn
	public String apiSecret;

	@SimpleIn
	public ClusterEvent[] events;

	@SimpleOut
	public int[] results;
	
	private String lastServer;
	
	private int tries;
	
	boolean clientMode;
	
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
	public String getHttpURL() {
		String lastServerPrefix = lastServer;
		if (lastServerPrefix != null) {
			String[] servers = ClusterConfig.clusterAllGatewayServers;
			if (servers != null && servers.length > 0) {
				int maxRandomTimes = servers.length * 5;
				do {
					int index = (int) Math.round(servers.length * Math.random());
					if (index >= servers.length) {
						index = 0;
					}
					String serverPrefix = servers[index];
					if (serverPrefix != null && (serverPrefix.startsWith("http://") || serverPrefix.startsWith("https://"))) {
						if (lastServerPrefix.equals(serverPrefix)
								&& servers.length > 1 // only one URL is configured
								&& maxRandomTimes > 0 // avoid incorrect pushServiceURLs with same URL
								) { 
							maxRandomTimes--;
							continue;
						}
						lastServer = serverPrefix;
						return serverPrefix + "/u/r";
					}
					maxRandomTimes--;
				} while (maxRandomTimes > 0);
			}
		}
		String serverPrefix = ClusterConfig.clusterGatewayServer;
		lastServer = serverPrefix;
		return serverPrefix + "/u/r";
	}

	@Override
	protected String[] fieldDiffIgnored() {
		// do not calculate field data when server responses.
		return new String[] { "event" };
	}
	
	@Override
	public void ajaxRun() {
		if (!ClusterServer.isClusterServer()) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: This server is not a cluster server!");
			}
			return; // silently
		}
		if (!XYZMixxer.isGatewaySecretOK(apiSecret)) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: API secret " + apiSecret + " authorization failed!");
			}
			return; // silently
		}
		if (events == null || events.length == 0) {
			return; // no return code
		}
		results = new int[events.length];
		for (int i = 0; i < events.length; i++) {
			ClusterEvent event = events[i];
			if (event == null || event.receiver <= 0) {
				continue;
			}
			/*
			if (ClusterConfig.clusterPrimaryServer && XYZMixxer.isPrimaryProxyMode()
					&& ClusterServer.getClusterNode() != null) {
				// Considered as gateway server, event will keep sender
			} else {
				event.setSender(0); // this event is not originated from this server
			}
			// */
			event.setSender(0); // sender is always ignored on gateway server
			results[i] = ClusterServer.pipeEvent(event);
		}
	}
	
	@Override
	public void ajaxIn() {
		super.ajaxIn();
		if (!clientMode) {
			return;
		}
		if (tries <= 0) {
			String[] servers = ClusterConfig.clusterAllGatewayServers;
			tries = Math.max(2, ClusterConfig.clusterMaxEventResendings) + (servers != null ? servers.length * 2 : 0);
		}
	}

	@Override
	public void ajaxFail() {
		super.ajaxFail();
		if (!clientMode) {
			return;
		}
		if (tries > 0) {
			tries--;
		}
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: Sending " + (events != null ? events.length : 0) + " events to gateway " + lastServer + " failed (" + tries + ")!");
		}
		if (tries > 0 && ClusterConfig.clusterMaxEventResendings > 0) {
			SimpleRPCRequest.request(this);
		} else {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: Giving up sending " + (events != null ? events.length : 0) + " events to gateway!");
			}
		}
	}
}
