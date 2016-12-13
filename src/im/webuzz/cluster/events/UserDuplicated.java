package im.webuzz.cluster.events;

import java.util.Map;

import net.sf.j2s.ajax.SimpleSerializable;

/**
 * User login elsewhere event is used to mark user logging from other server.
 * 
 * This event is only visible in Login2ServicePipe.
 * 
 * @author zhourenjian
 *
 */
public class UserDuplicated extends SimpleSerializable {

	private static String[] mappings = new String[] {
			"uid", "u",
			"domain", "d",
			"port", "p",
			"time", "t"
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);
	
	public int uid;
	
	public String domain;
	
	public int port;
	
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
