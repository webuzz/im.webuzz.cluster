package im.webuzz.cluster.events;

import im.webuzz.cluster.ClusterConfig;

import java.util.Map;

import net.sf.j2s.ajax.SimpleSerializable;

/**
 * User server result event is used to tell the given user is available on
 * given service server.
 * 
 * This event is only visible in Login2ServicePipe
 * 
 * @see UserQuery
 * 
 * @author zhourenjian
 *
 */
public class UserResult extends SimpleSerializable {

	private static String[] mappings = new String[] {
			"uid", "u",
			"domain", "d",
			"port", "p",
			"lastUpdated", "l",
			"status", "s"
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);
	
	public int uid; // Considered as [IN] from UserQuery 
	
	public String domain;
	
	public int port;
	
	public long lastUpdated;
	
	public int status; // Connected or disconnected
	
	private String[] callbacks;
	
	public void addCallback(String host) {
		if (host == null) {
			return;
		}
		String[] cb = callbacks;
		if (cb == null) {
			String[] newCallbacks = new String[ClusterConfig.clusterUserQueryCallbackSize];
			newCallbacks[0] = host;
			callbacks = newCallbacks;
		} else {
			int length = cb.length;
			for (int i = 0; i < length; i++) {
				if (cb[i] != null && host.equals(cb[i])) {
					return;
				}
			}
			for (int i = 0; i < length; i++) {
				if (cb[i] == null) {
					cb[i] = host;
					return;
				}
			}
			String[] newCallbacks = new String[length + (length >> 2) + ClusterConfig.clusterUserQueryCallbackSize];
			System.arraycopy(cb, 0, newCallbacks, 0, length);
			newCallbacks[length] = host;
			callbacks = newCallbacks;
		}
	}
	
	public void removeCallback(String host) {
		String[] cb = callbacks;
		if (cb == null || host == null) {
			return;
		}
		int length = cb.length;
		for (int i = 0; i < length; i++) {
			if (cb[i] != null && host.equals(cb[i])) {
				cb[i] = null;
				return;
			}
		}
	}
	
	public String[] getCallbacks() {
		return callbacks;
	}

	public void clearCallbacks() {
		callbacks = null;
	}
	
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
	
}
