package im.webuzz.cluster.events;

import java.util.Map;

import net.sf.j2s.ajax.SimpleSerializable;

/**
 * Service server is lost.
 * If service server is down, primary login servers need to remove all users
 * on this server.
 * 
 * @author zhourenjian
 *
 */
public class ServerLost extends SimpleSerializable {

	private static String[] mappings = new String[] {
			"domain", "d",
			"port", "p",
			"sourceDomain", "D",
			"sourcePort", "P",
			"slot", "s",
			"offlineBits", "o",
			"time", "t",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	/**
	 * Domain of service server which is just lost.
	 */
	public String domain;
	
	/**
	 * Port of service server which is just lost.
	 */
	public int port;

	/**
	 * Domain of primary login server which detects that service server is just lost.
	 */
	public String sourceDomain;
	
	/**
	 * Port of primary login server which detects that service server is just lost.
	 */
	public int sourcePort;
	
	public int slot;
	
	/**
	 * GZip encoded bits for all offline users in this slot.
	 * Each bit means a user at the its given location (user ID).
	 */
	public byte[] offlineBits;

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
	
	@Override
	protected boolean bytesCompactMode() {
		return true;
	}

}
