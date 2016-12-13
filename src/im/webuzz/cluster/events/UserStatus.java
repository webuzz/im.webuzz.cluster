package im.webuzz.cluster.events;

import java.util.Map;

import net.sf.j2s.ajax.SimpleSerializable;

/**
 * User server status event is used to tell login server that specified
 * user is connected or disconnected from this service server.
 * 
 * This event is only visible in Service2LoginPipe.
 * 
 * @see ServerStatus
 * 
 * @author zhourenjian
 *
 */
public final class UserStatus extends SimpleSerializable {

	private static String[] mappings = new String[] {
			"uid", "u",
			"status", "s",
			"time", "t"
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);
	
	public int uid;
	
	public int status; // CONNECTED, DISCONNECTED
	
	public long time;

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
