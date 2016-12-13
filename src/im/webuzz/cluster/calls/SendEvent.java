package im.webuzz.cluster.calls;

import java.util.Map;

import net.sf.j2s.ajax.annotation.SimpleIn;

import im.webuzz.cluster.ClusterEvent;
import im.webuzz.cluster.ClusterNode;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.xyz.XYZMixxer;

public class SendEvent extends AbstractClusterRPC {

	private static String[] mappings = new String[] {
			"apiSecret", "a",
			"serverPort", "p",
			"serverIP", "i",
			"returnCode", "r",
			"events", "e",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	@SimpleIn
	public ClusterEvent[] events;

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
	protected String[] fieldDiffIgnored() {
		// do not calculate field data when server responses.
		return new String[] { "events" };
	}
	
	@Override
	public void ajaxRun() {
		super.ajaxRun();
		if (returnCode != 0) {
			return;
		}
		
		if (events == null || events.length <= 0) {
			returnCode = AbstractClusterRPC.OK;
			return;
		}
		for (int i = 0; i < events.length; i++) {
			ClusterEvent event = events[i];
			if (event == null || event.receiver <= 0) {
				continue; // skip
			}
			ClusterNode serviceNode = ClusterServer.getClusterNode();
			if (serviceNode != null) {
				/*
				 * On receiving event from other cluster server, notify target user on
				 * this cluster node that there is an event.
				 */
				serviceNode.onReceivedEvent(event);
			}
			if (event.sender > 0) {
				XYZMixxer.fixQueryResult(event.sender, getRemoteDomain(), getRemotePort());
			}
		}
		returnCode = AbstractClusterRPC.OK;
	}

}
