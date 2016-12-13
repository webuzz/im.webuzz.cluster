package im.webuzz.cluster.calls;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterNode;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.events.IStatus;
import im.webuzz.cluster.events.ServerStatus;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.xyz.XYZMixxer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import net.sf.j2s.ajax.annotation.SimpleIn;
import net.sf.j2s.ajax.annotation.SimpleOut;

/*
 * Synchronize user statuses from master primary/login server.
 */
public class SynchronizeUsers extends AbstractClusterRPC {

	private static String[] mappings = new String[] {
			"apiSecret", "a",
			"serverPort", "p",
			"serverIP", "i",
			"returnCode", "r",
			"slot", "s",
			"onlineBits", "o",
			"serverIndexes", "v",
			"allServers", "l",
			"time", "t",
			"masterMode", "m",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	@SimpleIn
	public int slot;
	
	/**
	 * GZip encoded bits for all online users in this slot.
	 * Each bit means a user at the its given location (user ID).
	 */
	@SimpleOut
	public byte[] onlineBits;

	/**
	 * Ordered server index for each user in the online bit map.
	 * First value is server for the first user, who has 1-bit first in bit map.
	 */
	@SimpleOut
	public int[] serverIndexes;
	
	/**
	 * All online service servers. serverIndexes = 0 means the first server.
	 */
	@SimpleOut
	public ServerStatus[] allServers;
	
	@SimpleOut
	public long time;
	
	/**
	 * Whether this RPC is from master server or not.
	 * If it is from master, synchronize only sub-service servers' users.
	 */
	@SimpleIn
	public boolean masterMode;
	
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
	
	@Override
	public void ajaxRun() {
		super.ajaxRun();
		if (returnCode != 0) {
			return;
		}
		
		int maxUserID = slot;
		byte[] bits = new byte[(ClusterConfig.clusterMaxUsers >> (XYZMixxer.SLOTS_BITS + 3)) + 1];

		String host = ClusterConfig.port == 80 ? ClusterConfig.clusterServerIP
				: (ClusterConfig.clusterServerIP + ":" + ClusterConfig.port);
		Map<String, ServerStatus> serviceServers = XYZMixxer.getAllServiceServers(host, masterMode ? false : true);

		Map<Integer, Integer> userServerIndexes = new HashMap<Integer, Integer>(XYZMixxer.allStatuses[slot].size() / 4);
		Map<String, Integer> serverMap = new HashMap<String, Integer>();
		List<String> serverList = new ArrayList<String>(XYZMixxer.allServerStatuses.size());
		synchronized (XYZMixxer.statusMutex[slot]) {
			for (Iterator<UserResult> itr = XYZMixxer.allStatuses[slot].values().iterator(); itr.hasNext();) {
				UserResult usr = (UserResult) itr.next();
				if (usr.status == IStatus.CONNECTED) {
					// Only return online users
					String remoteHost = usr.port == 80 ? usr.domain : (usr.domain + ":" + usr.port);
					if (masterMode && !serviceServers.containsKey(remoteHost)) {
						boolean skipping = true;
						if (XYZMixxer.isPrimaryProxyMode()) {
							ClusterNode node = ClusterServer.getClusterNode();
							// For primary proxy and gateway mode, check if it is on gateway or not
							if (node != null && remoteHost.equals(node.getNodeID())) {
								// on gateway server
								skipping = false;
							}
						}
						if (skipping) {
							continue;
						}
					}
					if (/*masterMode && */XYZMixxer.isPrimaryProxyMode()) {
						// If master primary server try to synchronize users from proxy server,
						// change user to proxy server.
						remoteHost = host;
					}
					int uid = usr.uid;
					if (uid > maxUserID) {
						maxUserID = uid;
					}
					int location = (uid - slot) >> XYZMixxer.SLOTS_BITS;
					int index = location >> 3;
					int bit = location & 0x7;
					bits[index] |= 1 << bit;
					
					Integer serverIdx = serverMap.get(remoteHost);
					if (serverIdx == null) {
						serverIdx = Integer.valueOf(serverMap.size());
						serverMap.put(remoteHost, serverIdx);
						serverList.add(remoteHost);
					}
					userServerIndexes.put(uid, serverIdx);
				} // end of if CONNECTED
			} // end of for
			time = System.currentTimeMillis();
		} // end of synchronized block
		allServers = new ServerStatus[serverList.size()];
		for (int i = 0; i < allServers.length; i++) {
			String remoteHost = serverList.get(i);
			if (remoteHost != null) {
				ServerStatus ss = serviceServers.get(remoteHost);
				if (ss != null) {
					allServers[i] = ss;
				} else {
					if (ClusterConfig.clusterLogging) {
						System.out.println("Cluster: " + remoteHost + " is down!");
					}
				}
			}
		}
		// TODO: Use bits to compress server indexes
		// FIXME: SimpleSerializable's array has a limit of 1 million items
		int totalUsers = userServerIndexes.size();
		serverIndexes = new int[totalUsers];
		byte[] masks = new byte[8];
		for (int j = 0; j < 8; j++) {
			masks[j] = (byte) (1 << j);
		}
		int index = 0; // also current online user count
		for (int i = 0; i < bits.length; i++) {
			for (int j = 0; j < 8; j++) {
				if ((bits[i] & masks[j]) == masks[j]) {
					// user is online
					int uid = (((i << 3) + j) << XYZMixxer.SLOTS_BITS) + slot;
					Integer serverIndex = userServerIndexes.get(Integer.valueOf(uid));
					if (serverIndex != null) {
						serverIndexes[index] = serverIndex.intValue();
					}
					index++;
					if (index >= totalUsers) {
						break;
					}
				}
			}
			if (index >= totalUsers) {
				break;
			}
		}

		int size = ((maxUserID - slot) >> (XYZMixxer.SLOTS_BITS + 3)) + 1;
		//*
		// Make online bits more compact
		ByteArrayOutputStream out = new ByteArrayOutputStream(size / 4 + 16); // expect at least 25% compress rate
		GZIPOutputStream gZipOut = null;
		try {
			gZipOut = new GZIPOutputStream(out);
			gZipOut.write(bits, 0, size);
			returnCode = AbstractClusterRPC.OK;
		} catch (IOException e) {
			e.printStackTrace();
			returnCode = AbstractClusterRPC.ERROR;
		} finally {
			if (gZipOut != null) {
				try {
					gZipOut.close();
				} catch (IOException e) {
					e.printStackTrace();
					returnCode = AbstractClusterRPC.ERROR;
				}
			}
		}
		if (returnCode == AbstractClusterRPC.OK) {
			onlineBits = out.toByteArray();
		}
		// */
		/*
		// Raw data without gzip encoding
		onlineBits = new byte[size];
		System.arraycopy(bits, 0, onlineBits, 0, size);
		returnCode = AbstractClusterRPC.OK;
		// */
	}

}
