package im.webuzz.cluster.events;

import java.util.Map;

import net.sf.j2s.ajax.SimpleSerializable;

/**
 * Server status event that is used to tell service servers that a new server
 * is available or an old server is unavailable.
 * 
 * This event will be created by login/primary server, and will be piped down
 * to service server. Service servers will then know which server is available
 * and which is unavailable. Service servers will connect or disconnect to
 * server indicated in this event.
 * 
 * This event is only visible in Login2ServicePipe.
 * 
 * @see UserStatus
 * 
 * @author zhourenjian
 *
 */
public final class ServerStatus extends SimpleSerializable {

	private static String[] mappings = new String[] {
			"domain", "d",
			"port", "p",
			"status", "s"
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	public String domain;
	
	public int port;
	
	public int status; // CONNECTED, or DISCONNECTED

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
