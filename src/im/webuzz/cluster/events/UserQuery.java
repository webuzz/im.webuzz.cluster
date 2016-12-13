package im.webuzz.cluster.events;

import java.util.Map;

import net.sf.j2s.ajax.SimpleSerializable;

/**
 * User server query event is used to find out which service server is the
 * given user get logged in.
 * 
 * This event is only visible in Service2LoginPipe.
 * 
 * @see UserResult
 * 
 * @author zhourenjian
 *
 */
public class UserQuery extends SimpleSerializable {

	private static String[] mappings = new String[] {
			"uid", "u",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);
	
	public int uid;

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
