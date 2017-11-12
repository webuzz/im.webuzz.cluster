package im.webuzz.cluster.xyz;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterEvent;
import im.webuzz.cluster.ClusterMappings;
import im.webuzz.cluster.ClusterNode;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.calls.ClosePipe;
import im.webuzz.cluster.calls.SendEvent;
import im.webuzz.cluster.calls.SynchronizeUsers;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.events.IStatus;
import im.webuzz.cluster.events.ServerLost;
import im.webuzz.cluster.events.ServerStatus;
import im.webuzz.cluster.events.ServerStopping;
import im.webuzz.cluster.events.UserDuplicated;
import im.webuzz.cluster.events.UserQuery;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.events.UserStatus;
import im.webuzz.cluster.pipes.AbstractClusterPipe;
import im.webuzz.cluster.pipes.Login2LoginPipe;
import im.webuzz.cluster.pipes.Login2ServicePipe;
import im.webuzz.cluster.pipes.Service2LoginPipe;
import im.webuzz.cluster.pipes.Service2ServicePipe;
import im.webuzz.threadpool.SimpleThreadPoolExecutor;
import im.webuzz.threadpool.ThreadPoolExecutorConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.sf.j2s.ajax.SimplePipeRequest;
import net.sf.j2s.ajax.SimpleRPCRequest;
import net.sf.j2s.ajax.SimpleSerializable;

/**
 * Mix calls, events and pipes together to make a cluster server.
 * 
 * @author zhourenjian
 *
 */
public class XYZMixxer {

	private static SimpleThreadPoolExecutor executor = null;

	private static Map<String, Login2LoginPipe> allLogin2LoginPipes = new ConcurrentHashMap<String, Login2LoginPipe>();
	private static Login2ServicePipe loginPipe;
	
	private static Map<String, Service2ServicePipe> allService2ServicePipes = new ConcurrentHashMap<String, Service2ServicePipe>();
	private static Map<String, Service2LoginPipe> allService2LoginPipes = new ConcurrentHashMap<String, Service2LoginPipe>();
	
	// keep local value and we use *.ini to trigger updates
	private static String masterServerIP = null;
	private static int masterServerPort = 80;
	private static String primaryGlobalServer = null;

	public static final int SLOTS = 128;
	public static final int SLOTS_MASKS = 0x7f;
	public static final int SLOTS_BITS = 7;
	
	// for primary login server
	public static Map<String, ServerStatus> allServerStatuses = new ConcurrentHashMap<String, ServerStatus>();
	
	// for primary login server
	public static Object[] statusMutex;
	public static Map<Object, UserResult>[] allStatuses;
	// for service server
	public static Object[] dataMutex;
	public static Map<Object, List<ClusterEvent>>[] crossPipeData;
	// for service server
	public static Object[] userMutex;
	public static Map<Object, UserResult>[] cachedUsers;

	private static ISynchronizedCallback syncedCallback = null;
	
	private static boolean initialized = false;
	private static long initializedTime = -1;
	
	private static ThreadPoolExecutorConfig lastConfig = ClusterConfig.clusterWorkerPool;

	private static String lastGlobalServer = null;
	
	@SuppressWarnings("unchecked")
	public static void initialize() {
		if (initialized) {
			return;
		}
		statusMutex = new Object[SLOTS];
		allStatuses = new Map[SLOTS];
	
		dataMutex = new Object[SLOTS];
		crossPipeData = new Map[SLOTS];
		
		userMutex = new Object[SLOTS];
		cachedUsers = new Map[SLOTS];
		
		for (int i = 0; i < SLOTS; i++) {
			statusMutex[i] = new Object();
			allStatuses[i] = new HashMap<Object, UserResult>(ClusterConfig.clusterInitialCapacity);
		
			dataMutex[i] = new Object();
			crossPipeData[i] = new HashMap<Object, List<ClusterEvent>>(ClusterConfig.clusterInitialCapacity);
			
			userMutex[i] = new Object();
			cachedUsers[i] = new HashMap<Object, UserResult>(ClusterConfig.clusterInitialCapacity);
		}
		ClusterMappings.initializeMappings();
		initialized = true;
		initializedTime = System.currentTimeMillis();
		lastConfig = ClusterConfig.clusterWorkerPool;
		if (lastConfig == null) {
			lastConfig = new ThreadPoolExecutorConfig();
			lastConfig.workerName = "Cluster Event Callback Worker";
		}
		executor = new SimpleThreadPoolExecutor(lastConfig);
		executor.allowCoreThreadTimeOut(lastConfig.threadTimeout);
	}
	
	public static void updatePoolConfigurations() {
		if (!initialized || executor == null) {
			return;
		}
		ThreadPoolExecutorConfig wc = ClusterConfig.clusterWorkerPool;
		if (wc != null) {
			wc.updatePoolWithComparison(executor, lastConfig);
			lastConfig = wc;
		}
	}

	public static boolean isJustInitialized() {
		return Math.abs(System.currentTimeMillis() - initializedTime) < ClusterConfig.clusterMaxInitializeTime; 
	}
	
	public static boolean isAPISecretOK(String apiSecret) {
		if (apiSecret == null) {
			if (ClusterConfig.clusterAPISecret == null && ClusterConfig.clusterAllAPISecrets == null) {
				return true;
			}
			return false;
		}
		if (ClusterConfig.clusterAPISecret != null
				&& ClusterConfig.clusterAPISecret.equals(apiSecret)) {
			return true;
		}
		String[] keys = ClusterConfig.clusterAllAPISecrets;
		if (keys != null) {
			for (int i = 0; i < keys.length; i++) {
				if (apiSecret.equals(keys[i])) {
					return true;
				}
			}
		}
		return false;
	}
	
	public static boolean isGatewaySecretOK(String gatewaySecret) {
		if (gatewaySecret == null) {
			if (ClusterConfig.clusterAPISecret == null && ClusterConfig.clusterAllAPISecrets == null
					&& ClusterConfig.clusterGatewaySecret == null && ClusterConfig.clusterAllGatewaySecrets == null) {
				return true;
			}
			return false;
		}
		if (gatewaySecret.equals(ClusterConfig.clusterAPISecret)) {
			return true;
		}
		String[] keys = ClusterConfig.clusterAllAPISecrets;
		if (keys != null) {
			for (int i = 0; i < keys.length; i++) {
				if (gatewaySecret.equals(keys[i])) {
					return true;
				}
			}
		}
		if (gatewaySecret.equals(ClusterConfig.clusterGatewaySecret)) {
			return true;
		}
		String[] gatewayKeys = ClusterConfig.clusterAllGatewaySecrets;
		if (gatewayKeys != null) {
			for (int i = 0; i < gatewayKeys.length; i++) {
				if (gatewaySecret.equals(gatewayKeys[i])) {
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Pipe out events according to given user server result.
	 * 
	 * @param r
	 * @param events
	 * @return
	 */
	public static boolean pipeOut(String remoteHost, ClusterEvent... events) {
		Service2ServicePipe pipe = Service2ServicePipe.allServicePipes.get(remoteHost);
		if (pipe != null) {
			pipe.pipeThrough(events);
			return true;
		}
		String localHost = ClusterConfig.port == 80 ? ClusterConfig.clusterServerIP
				: (ClusterConfig.clusterServerIP + ":" + ClusterConfig.port);
		ClusterNode node = ClusterServer.getClusterNode();
		if (node != null && (localHost.equals(remoteHost) // local server
				|| (remoteHost != null && remoteHost.equals(node.getNodeID())))) { // gateway node
			for (int i = 0; i < events.length; i++) {
				node.onReceivedEvent(events[i]);
			}
			return true;
		}
		// Ignore events
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: Not getting pipe for " + remoteHost);
		}
		return false;
	}

	/**
	 * Update service server to switch primary global server URL prefix.
	 * In case new primary server is available, we can switch all service servers to new server
	 * and then update old primary server.
	 */
	public static void updatePrimaryGlobalServer(String globalServer, String[] backupServers) {
		if (!initialized) {
			return; // Not initialized yet
		}
		boolean existed = false;
		if (primaryGlobalServer != null) {
			if (primaryGlobalServer.equals(globalServer)) {
				existed = true;
			} else if (backupServers != null) {
				for (String serverPrefix : backupServers) {
					if (primaryGlobalServer.equals(serverPrefix)) {
						existed = true;
						break;
					}
				}
			}
		}
		if (existed && lastGlobalServer != null && lastGlobalServer.equals(globalServer)) {
			return; // already initialized and global server have not been changed yet.
		}
		if ((globalServer == null && primaryGlobalServer != null)
				|| (globalServer != null && !globalServer.equals(primaryGlobalServer))) {
			final Login2ServicePipe p = loginPipe;
			final String httpURL = p != null ? p.getHttpURL() : ((primaryGlobalServer == null ? ClusterConfig.clusterGlobalServer : primaryGlobalServer) + "/u/r");
			
			// need update
			primaryGlobalServer = globalServer;

			// Disconnect service server from current primary login server, then
			// try to close current Login2ServicePipe, and Login2ServicePipe
			// will try to reconnect primary server with the updated
			// primaryGlobalServer value.
			// On reconnected, new primary server will create Service2LoginPipe.
			Service2LoginPipe servicePipe = null;
			synchronized (Service2LoginPipe.pipeMutex) { // get correct service pipe across multiple threads
				servicePipe = Service2LoginPipe.servicePipe;
			}
			if (servicePipe != null) {
				servicePipe.pipeThrough(new ServerStopping()); // Client won't try to reconnect
				ClosePipe.closePipe(servicePipe.pipeKey);
			}
			if (p != null && p.pipeKey != null/* && p.isPipeLive()*/) {
				ClosePipe r = new ClosePipe() {
					
					@Override
					public String getHttpURL() {
						return httpURL;
					}
					
					@Override
					public void ajaxOut() {
						// do nothing, let pipe closed by remote server
						// p.pipeClosed();
						p.pipeReset(); // #pipeReset method is override, specially reset cached URL prefix
					}
					
					@Override
					public void ajaxFail() {
						p.pipeClosed();
					}
					
				};
				r.apiSecret = ClusterConfig.clusterAPISecret;
				r.pipeKey = p.pipeKey;
				r.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
				SimpleRPCRequest.request(r);
			} else if (p != null) { // should never reach this branch
				p.pipeReset(); // #pipeReset method is override, specially reset cached URL prefix
			}
		}
	}

	/**
	 * Update primary/login server to a new master server.
	 * We can make a primary/login server stand-alone/master or slave on demand.
	 */
	public static void updatePrimaryMasterServer(final String serverIP, final int serverPort) {
		if (!initialized) {
			return; // Not initialized yet
		}
		if (serverPort != masterServerPort
				|| (serverIP == null && masterServerIP != null)
				|| (serverIP != null && !serverIP.equals(masterServerIP))) {
			// need update
			// stop early existed synchronizing pipe
			destroyLogin2LoginPipe(masterServerIP, masterServerPort);
			if (serverPort == ClusterConfig.port && serverIP != null
					&& serverIP.equals(ClusterConfig.clusterServerIP)) {
				// do nothing, now this server is a stand-alone primary login server.
			} else {
				// if synchronizing pipe is not started, try to start it
				// if synchronizing pipe is started, try to switch to new pipe
				createLogin2LoginPipe(ClusterConfig.clusterServerIP,
						serverIP, serverPort, ClusterConfig.clusterPrimarySynchronizing, false);
				
				// The following XYZMixxer.synchronizeSlots will be invoke on Login2LoginPipe#pipeCreated
				// Only after Login2LoginPipe is created then start synchronizing, or user status may be
				// inaccurate if users update statuses in between slot already be synchronized and pipe not
				// be created.
				/*
				if (ClusterConfig.clusterPrimarySynchronizing) {
					// Start synchronizing slots task
					synchronizeSlots(serverIP, serverPort, false, 0);
				}
				// */
			}
			masterServerIP = serverIP;
			masterServerPort = serverPort;
		}
	}

	/**
	 * Proxy mode of primary/login server.
	 * Some primary login server may perform as proxy server between login
	 * servers and service servers.
	 */
	public static boolean isPrimaryProxyMode() {
		return ClusterConfig.clusterPrimaryServer && !ClusterConfig.clusterPrimaryLoginServer && ClusterConfig.clusterPrimaryProxyServer;
	}

	/**
	 * Master primary login server or slave server.
	 * @return
	 */
	public static boolean isPrimaryMasterMode() {
		return ClusterConfig.clusterPrimaryServer
				&& !ClusterConfig.clusterPrimaryProxyServer
				&& masterServerPort == ClusterConfig.port
				&& masterServerIP != null && masterServerIP.equals(ClusterConfig.clusterServerIP);
	}
	
	public static void setSynchronizedCallback(ISynchronizedCallback callback) {
		syncedCallback = callback;
	}
	
	/*
	 * Synchronize slots from remote primary/login servers on given slot.
	 */
	public static void synchronizeSlots(final String remoteDomain, final int remotePort, final boolean syncMode, final int startingSlot) {
		final String remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
		SynchronizeUsers r = new SynchronizeUsers() {
			
			@Override
			public String getHttpURL() {
				return "http://" + remoteHost + "/u/r";
			}
	
			@Override
			public void ajaxOut() {
				if (returnCode != OK || onlineBits == null
						|| serverIndexes == null || allServers == null) {
					retryOrFallback();
					return;
				}
				// gzip decoding is considered as CPU critical.
				// Run it in another thread avoiding blocking other RPCs
				XYZMixxer.runTask(new Runnable() {
					
					@Override
					public void run() {
						//*
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						ByteArrayInputStream bais = new ByteArrayInputStream(onlineBits);
						GZIPInputStream gis = null;
						try {
							gis = new GZIPInputStream(bais);
							byte[] buffer = new byte[8096];
							int read = -1;
							while ((read = gis.read(buffer)) > 0) {
								baos.write(buffer, 0, read);
							}
						} catch (Throwable e) {
							e.printStackTrace();
						} finally {
							if (gis != null) {
								try {
									gis.close();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
						byte[] bits = baos.toByteArray();
						// */
						/*
						// raw online bits
						byte[] bits = onlineBits;
						// */
						byte[] masks = new byte[8];
						for (int j = 0; j < 8; j++) {
							masks[j] = (byte) (1 << j);
						}
						int index = 0; // also current online user count
						for (int i = 0; i < bits.length; i++) {
							for (int j = 0; j < 8; j++) {
								if ((bits[i] & masks[j]) == masks[j]) {
									// user is online
									int serverIdx = -1;
									if (index < serverIndexes.length) {
										serverIdx = serverIndexes[index];
									}
									if (serverIdx >= 0 && serverIdx < allServers.length) {
										ServerStatus ss = allServers[serverIdx];
										if (ss != null) {
											int uid = (((i << 3) + j) << SLOTS_BITS) + slot;
											UserStatus uss = new UserStatus();
											uss.status = IStatus.CONNECTED;
											uss.time = time;
											uss.uid = uid;
											updateUserOnServer(uss, ss.domain, ss.port);
										}
									}
									index++;
								}
							}
						}
						if (startingSlot < XYZMixxer.SLOTS - 1) {
							synchronizeSlots(remoteDomain, remotePort, syncMode, startingSlot + 1);
						}  else { // else all slots synchronized
							ISynchronizedCallback cb = syncedCallback;
							if (cb != null) {
								try {
									cb.allSlotsSynchronized();
								} catch (Throwable e) {
									e.printStackTrace();
								}
							}
							if (ClusterConfig.clusterLogging) {
								System.out.println("Cluster: All slots of server " + remoteHost + " synchronized " + System.currentTimeMillis());
							}
						}
					}
					
				});
			}
			
			@Override
			public void ajaxFail() {
				retryOrFallback();
			}
			
			@Override
			protected void onFallback() {
				if (allLogin2LoginPipes.get(remoteHost) == null) {
					return; // breaking synchronizing...
				}
				synchronizeSlots(remoteDomain, remotePort, syncMode, startingSlot);
			}
			
		};
		r.masterMode = syncMode;
		r.slot = startingSlot;
		r.apiSecret = ClusterConfig.clusterAPISecret;
		r.setRetries(3); // We would like to make more tries
		r.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		SimpleRPCRequest.request(r);
	}
	
	/**
	 * Disconnect from primary login server as a slave login server.
	 * 
	 * @param remoteDomain
	 * @param remotePort
	 */
	private static void destroyLogin2LoginPipe(String remoteDomain, int remotePort) {
		if (remoteDomain == null) { // no pipe for null domain
			return;
		}
		final String remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
		Login2LoginPipe l2lPipe = Login2LoginPipe.allSyncPipes.get(remoteHost);
		if (l2lPipe != null) {
			l2lPipe.pipeThrough(new ServerStopping()); // Client won't try to reconnect
			ClosePipe.closePipe(l2lPipe.pipeKey);
		}
		final Login2LoginPipe p = allLogin2LoginPipes.get(remoteHost);
		if (p != null/* && p.isPipeLive()*/) {
			p.setStopping(true); // not trying to reconnect
			ClosePipe r = new ClosePipe() {
				
				@Override
				public String getHttpURL() {
					return p.getHttpURL();
				}
				
				@Override
				public void ajaxOut() {
					// do nothing, let pipe closed by remote server
					// p.pipeClosed();
				}
				
				@Override
				public void ajaxFail() {
					p.pipeClosed();
				}

			};
			r.apiSecret = ClusterConfig.clusterAPISecret;
			r.pipeKey = p.pipeKey;
			r.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
			SimpleRPCRequest.request(r);
			return; // ignore
		}
	}
	
	/**
	 * Connect primary login server, synchronizing as a slave login server.
	 * 
	 * @param targetDomain
	 * @param remoteDomain
	 * @param remotePort
	 */
	public static void createLogin2LoginPipe(String targetDomain,
			final String remoteDomain, final int remotePort, boolean synchronizing, final boolean syncMode) {
		final String remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
		Login2LoginPipe p = allLogin2LoginPipes.get(remoteHost);
		if (p != null/* && p.isPipeLive()*/) {
			return; // ignore
		} // else p is null
		p = new Login2LoginPipe() {
	
			@Override
			public String getPipeURL() {
				return "http://" + remoteHost + "/u/p";
			}
	
			@Override
			public String getHttpURL() {
				return "http://" + remoteHost + "/u/r";
			}
	
			@Override
			public void keepPipeLive() {
				setLastHeartbeat(System.currentTimeMillis());
				super.keepPipeLive();
			}
	
			@Override
			public boolean isPipeLive() {
				return super.isPipeLive() && Math.abs(System.currentTimeMillis() - getLastHeartbeat()) < ClusterConfig.clusterPipeHeartBeatTimeoutInterval;
			}
			
			@Override
			public boolean deal(HeartBeat shb) {
				setLastHeartbeat(System.currentTimeMillis());
				return super.deal(shb);
			}
			
			@Override
			public boolean deal(UserResult usr) {
				// User status updates from other login server
				UserStatus uss = new UserStatus();
				uss.status = usr.status;
				uss.time = usr.lastUpdated;
				uss.uid = usr.uid;
				if (updateUserOnServer(uss, usr.domain, usr.port)
						&& ClusterConfig.clusterPrimarySynchronizing) { // Proxy to other servers
					// Events are from other primary server, it should already compromised with primary proxy mode
					for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
							itr.hasNext();) {
						Login2LoginPipe p = (Login2LoginPipe) itr.next();
						if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
							// UserResult event is not coming from the same server of this pipe
							p.pipeThrough(usr);
						}
					}
				}
				return true;
			}
			
			@Override
			public boolean deal(ServerStatus ss) {
				// Server status updates from other login server
				String remoteHost = ss.port == 80 ? ss.domain : (ss.domain + ":" + ss.port);
				ServerStatus s = null;
				if (ss.status == IStatus.DISCONNECTED) {
					s = allServerStatuses.remove(remoteHost);
				} else {
					s = allServerStatuses.put(remoteHost, ss);
				}
				if (s == null || (s != null && s.status == IStatus.DISCONNECTED && ss.status == IStatus.CONNECTED)) {
					// new server or server is back online, notify servers
					if (!XYZMixxer.isPrimaryProxyMode()) {
						for (Iterator<Login2ServicePipe> itr = Login2ServicePipe.allLoginPipes.values().iterator(); itr.hasNext();) {
							Login2ServicePipe pipe = itr.next();
							if (!remoteHost.equals(pipe.getHost())) {
								// pipe this pipe's another end (service server) to all existed
								// service server
								pipe.pipeThrough(ss);
							}
						}
					} // else: In primary proxy mode, outer service servers should not be known by inner servers
					if (ClusterConfig.clusterPrimarySynchronizing) {
						// Events are from other primary server, it should already compromised with primary proxy mode
						for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
								itr.hasNext();) {
							Login2LoginPipe p = (Login2LoginPipe) itr.next();
							if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
								// ServerStatus event is not coming from the same server of this pipe
								if (ss.port != p.port || (ss.domain != null && !ss.domain.equals(p.domain))) {
									// ServerStatus event is not being generated from the same server of this pipe
									p.pipeThrough(ss);
								}
							}
						}
					}
				}
				if (ss.status == IStatus.CONNECTED && !ClusterConfig.clusterPrimaryLoginServer
						&& (ss.port != ClusterConfig.port || (ss.domain != null
								&& !ss.domain.equals(ClusterConfig.clusterServerIP)))) {
					createService2ServicePipe(ss.domain, ss.port);
				}
				return true;
			}
			
			@Override
			public boolean deal(final ServerLost sl) {
				if (sl.offlineBits == null || sl.offlineBits.length <= 0) {
					return true;
				}
				// gzip decoding is considered as CPU critical.
				// Run it in another thread avoiding blocking other RPCs
				XYZMixxer.runTask(new Runnable() {
					
					@Override
					public void run() {
						//*
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						ByteArrayInputStream bais = new ByteArrayInputStream(sl.offlineBits);
						GZIPInputStream gis = null;
						try {
							gis = new GZIPInputStream(bais);
							byte[] buffer = new byte[8096];
							int read = -1;
							while ((read = gis.read(buffer)) > 0) {
								baos.write(buffer, 0, read);
							}
						} catch (Throwable e) {
							e.printStackTrace();
						} finally {
							if (gis != null) {
								try {
									gis.close();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
						byte[] bits = baos.toByteArray();
						// */
						/*
						// raw online bits
						byte[] bits = sl.offlineBits;
						// */
						byte[] masks = new byte[8];
						for (int j = 0; j < 8; j++) {
							masks[j] = (byte) (1 << j);
						}
						boolean containsUpdate = false;
						// int index = 0; // also current online user count
						for (int i = 0; i < bits.length; i++) {
							for (int j = 0; j < 8; j++) {
								if ((bits[i] & masks[j]) == masks[j]) {
									// user is offline
									int uid = (((i << 3) + j) << SLOTS_BITS) + sl.slot;
									UserStatus uss = new UserStatus();
									uss.status = IStatus.DISCONNECTED;
									uss.time = sl.time;
									uss.uid = uid;
									if (updateUserOnServer(uss, sl.domain, sl.port)) {
										containsUpdate = true;
									}
									// index++;
								}
							}
						}

						if (containsUpdate && ClusterConfig.clusterPrimarySynchronizing) {
							// ServerLost event needs to be piped to other primary server
							for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
									itr.hasNext();) {
								Login2LoginPipe p = (Login2LoginPipe) itr.next();
								if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
									// ServerLost event is not coming from the same server of this pipe
									if (sl.sourcePort != p.port || (sl.sourceDomain != null && !sl.sourceDomain.equals(p.domain))) {
										// Not sending ServerLost event back to its source
										p.pipeThrough(sl);
									}
								}
							}
						}
					}
					
				});
				return true;
			}
			
			@Override
			public boolean deal(ServerStopping ssp) {
				setStopping(true);
				return true;
			}
			
			@Override
			public void pipeCreated() {
				super.pipeCreated();
				setStopping(false);
				setLastHeartbeat(System.currentTimeMillis());
				if (onlineServers != null) {
					for (int i = 0; i < onlineServers.length; i++) {
						ServerStatus ss = onlineServers[i];
						if (ss != null) {
							deal(ss);
						}
					}
				}
				if (isSynchronizing()) { // need syncrhonzing from remote server
					setSynchronizing(false);
					if (ClusterConfig.clusterLogging) {
						System.out.println("Cluster: Start synchronizing slots of server " + remoteHost + " " + System.currentTimeMillis());
					}
					synchronizeSlots(remoteDomain, remotePort, syncMode, 0);
				}
			}
			
			@Override
			public void pipeClosed() {
				super.pipeClosed();
				if (isStopping()) {
					pipeRemoved();
					return;
				}
				if (allLogin2LoginPipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeLost() {
				super.pipeLost();
				if (isStopping()) {
					pipeRemoved();
					return;
				}
				if (allLogin2LoginPipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeFailed() {
				super.pipeFailed();
				if (isStopping()) {
					pipeRemoved();
					return;
				}
				if (allLogin2LoginPipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
	
			@Override
			protected void pipeRemoved() {
				removeClusterPipeFromMonitor4Client(this);
				allLogin2LoginPipes.remove(remoteHost);
				ServerStatus ss = allServerStatuses.remove(remoteHost); // Remote server may be proxy server
				if (ClusterConfig.clusterPrimarySynchronizing && ss != null) {
					// remote primary server is a proxy server, as server status map contain this server
					ss = new ServerStatus();
					ss.domain = remoteDomain;
					ss.port = remotePort;
					ss.status = IStatus.DISCONNECTED;
					// Events are from other primary server, it should already compromised with primary proxy mode
					for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
							itr.hasNext();) {
						Login2LoginPipe p = (Login2LoginPipe) itr.next();
						if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
							// ServerStatus event is not being generated from the same server of this pipe
							p.pipeThrough(ss);
						}
					}
				}
				super.pipeRemoved();
			}
			
		};
		p.setSynchronizing(synchronizing);
		p.setLastHeartbeat(System.currentTimeMillis());
		allLogin2LoginPipes.put(remoteHost, p);
		p.apiSecret = ClusterConfig.clusterAPISecret;
		p.port = ClusterConfig.port;
		// on triggered by *.ini update (server#1)
		// #createLogin2LoginPipe, targetDomain is ClusterConfig#clusterServerIP, set to p#domain
		// remoteDomain is ClusterConfig#clusterMasterServerIP, set to p#target
		// pipe created on calling #createLogin2LoginPipe
		//  
		// on triggered by pipeSetup (server#2)
		// #createLogin2Login, targetDomain is fixed server IP, set to p#domain
		// remoteDomain is remote primary server's domain, set to p#target
		// pipe created on calling #createLogin2LoginPipe
		//
		// on triggered by pipeSetup (server#1)
		// #createLogin2LoginPipe, targetDomain is fixed server IP, set to p#domain
		// remoteDomain is remote primary server's domain, set to p#target
		// pipe existed on calling #createLogin2LoginPipe
		p.domain = targetDomain;
		p.target = remoteDomain;
		AbstractClusterPipe.startClusterPipeMonitor4Client(p);
		p.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		SimplePipeRequest.pipe(p);
	}

	/**
	 * Connect and register to login server (primary server).
	 */
	public static void createLogin2ServicePipe() {
		Login2ServicePipe p = new Login2ServicePipe() {
	
			private String urlPrefix = null;
			
			private int failedRetries;
			private long firstFailedTime;
			private int lastRPCIndex;
			private String preferredURLPrefix;
			private String requestingURLPrefix;

			public String getPreferredURLPrefix() {
				return primaryGlobalServer == null ? ClusterConfig.clusterGlobalServer : primaryGlobalServer;
			}

			@Override
			public String getPipeURL() {
				/*
				 * urlPrefix will be assigned in #getHttpURL
				 */
				if (urlPrefix == null) { // #pipeReset is invoked and pipe should be closed sooner?
					return requestingURLPrefix + "/u/p";
				}
				return urlPrefix + "/u/p";
			}
	
			@Override
			public String getHttpURL() {
				if (urlPrefix != null) {
					// keep url prefix
				} else if (preferredURLPrefix != null && (preferredURLPrefix.startsWith("http://") || preferredURLPrefix.startsWith("https://"))) {
					urlPrefix = preferredURLPrefix;
					requestingURLPrefix = urlPrefix;
				} else {
					urlPrefix = getPreferredURLPrefix();
					requestingURLPrefix = urlPrefix;
				}
				return urlPrefix + "/u/r";
			}

			@Override
			public void pipeReset() {
				// super.pipeReset(); // No super#pipeReset invoking, which may change destoryed value
				urlPrefix = null;
				preferredURLPrefix = null;
			}
			
			@Override
			public void ajaxFail() {
				super.ajaxFail();
				failedRetries++;
				if (firstFailedTime <= 0) {
					firstFailedTime = System.currentTimeMillis();
				}
				if (failedRetries <= ClusterConfig.clusterMinReconnectRetries || System.currentTimeMillis() - firstFailedTime < ClusterConfig.clusterMaxReconnectInterval) {
					// Not enough tries and not long enough for detecting remote server is down.
					return;
				}
				if (ClusterConfig.clusterLogging) {
					System.out.println("Cluster: Failed to reconnect " + this.getClass().getSimpleName() + " with " + failedRetries + " retries and cost " + (System.currentTimeMillis() - firstFailedTime) + "ms. Try to switch to other possible server.");
				}
				urlPrefix = null;
				preferredURLPrefix = null;
				String[] backendURLPrefixes = ClusterConfig.clusterGlobalBackupServers; // no modification
				if (ClusterConfig.clusterBackupServerStrategy == 1) { // order looping
					if (backendURLPrefixes != null && backendURLPrefixes.length > 0) {
						if (lastRPCIndex < 0) {
							lastRPCIndex = 0;
						}
						String nextPrefix = null;
						for (int i = lastRPCIndex; i < backendURLPrefixes.length; i++) {
							String prefix = backendURLPrefixes[i];
							if (requestingURLPrefix.equals(prefix)) {
								continue;
							}
							if (prefix != null && prefix.length() > 0 && (prefix.startsWith("http://") || prefix.startsWith("https://"))) {
								lastRPCIndex = i;
								nextPrefix = prefix;
								break;
							}
						}
						if (nextPrefix != null) {
							preferredURLPrefix = nextPrefix;
						} else {
							lastRPCIndex = -1;
							preferredURLPrefix = getPreferredURLPrefix();
						}
					} // else last url prefix, same as #getPreferredURLPrefix
				} else {
					if (backendURLPrefixes != null && backendURLPrefixes.length > 0) {
						int maxRandomTimes = backendURLPrefixes.length * ClusterConfig.clusterBackupServerRandomFactor;
						do {
							int index = (int) Math.round(backendURLPrefixes.length * Math.random());
							if (index >= backendURLPrefixes.length) {
								index = 0;
							}
							if (index == lastRPCIndex) {
								maxRandomTimes--;
								continue;
							}
							String urlPrefix = backendURLPrefixes[index];
							if (urlPrefix != null && (urlPrefix.startsWith("http://") || urlPrefix.startsWith("https://"))) {
								if (requestingURLPrefix != null && requestingURLPrefix.equals(urlPrefix)
										&& backendURLPrefixes.length > 1 // only one URL is configured
										&& maxRandomTimes > 0 // avoid incorrect pushServiceURLs with same URL
										) { 
									maxRandomTimes--;
									continue;
								}
								preferredURLPrefix = urlPrefix;
								lastRPCIndex = index;
								break;
							}
							maxRandomTimes--;
						} while (maxRandomTimes > 0);
					}
				}
			}
	
			@Override
			public void keepPipeLive() {
				setLastHeartbeat(System.currentTimeMillis());
				super.keepPipeLive();
			}
			
			@Override
			public boolean isPipeLive() {
				return super.isPipeLive() && Math.abs(System.currentTimeMillis() - getLastHeartbeat()) < ClusterConfig.clusterPipeHeartBeatTimeoutInterval;
			}
			
			@Override
			public boolean deal(HeartBeat shb) {
				setLastHeartbeat(System.currentTimeMillis());
				return super.deal(shb);
			}
	
			@Override
			public boolean deal(ServerStatus ss) {
				if (ClusterConfig.clusterServerIP != null && ClusterConfig.clusterServerIP.equals(ss.domain)
						&& ClusterConfig.port == ss.port) {
					// Ignore this server's status update
					return true;
				}
				if (ss.status == IStatus.CONNECTED) {
					XYZMixxer.createService2ServicePipe(ss.domain, ss.port);
				}
				String remoteHost = ss.port == 80 ? ss.domain : (ss.domain + ":" + ss.port);
				allServerStatuses.put(remoteHost, ss);
				return true;
			}
	
			/**
			 * @see Service2LoginPipe#deal(UserQuery)
			 */
			@Override
			public boolean deal(UserResult usr) {
				gotUserOnServer(usr);
				return true;
			}
			
			@Override
			public boolean deal(UserDuplicated usd) {
				notifyDuplicatedLogin(usd.uid, usd.time, usd.domain, usd.port);
				return true;
			}
			
			@Override
			public void pipeCreated() {
				super.pipeCreated();
				setLastHeartbeat(System.currentTimeMillis());
				if (primaryGlobalServer != null && urlPrefix != null && !primaryGlobalServer.equals(urlPrefix)) {
					primaryGlobalServer = urlPrefix;
				}
				failedRetries = 0;
				firstFailedTime = 0;
			}
			
			@Override
			public void pipeClosed() {
				super.pipeClosed();
				if (loginPipe != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeLost() {
				super.pipeLost();
				if (loginPipe != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeFailed() {
				super.pipeFailed();
				if (loginPipe != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			protected void pipeRemoved() {
				// do nothing, and keep this pipe reconnecting
			}
			
		};
		p.apiSecret = ClusterConfig.clusterAPISecret;
		p.port = ClusterConfig.port;
		p.domain = ClusterConfig.clusterServerIP;
		p.setLastHeartbeat(System.currentTimeMillis());
		AbstractClusterPipe.startClusterPipeMonitor4Client(p);
		p.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		loginPipe = p;
		SimplePipeRequest.pipe(p);
	}

	/**
	 * Connect to service server for incoming user server status updates
	 * and user server query.
	 * 
	 * For primary server / login server only.
	 * 
	 * @param remoteDomain
	 * @param remotePort
	 */
	public static void createService2LoginPipe(final String remoteDomain, final int remotePort) {
		final String remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
		Service2LoginPipe p = allService2LoginPipes.get(remoteHost);
		if (p != null/* && p.isPipeLive()*/) {
			return; // ignore
		} // else p is null
		p = new Service2LoginPipe() {
	
			@Override
			public String getPipeURL() {
				return "http://" + remoteHost + "/u/p";
			}
	
			@Override
			public String getHttpURL() {
				return "http://" + remoteHost + "/u/r";
			}
	
			@Override
			public void keepPipeLive() {
				setLastHeartbeat(System.currentTimeMillis());
				super.keepPipeLive();
			}
	
			@Override
			public boolean isPipeLive() {
				return super.isPipeLive() && Math.abs(System.currentTimeMillis() - getLastHeartbeat()) < ClusterConfig.clusterPipeHeartBeatTimeoutInterval;
			}
			
			@Override
			public boolean deal(HeartBeat shb) {
				Login2ServicePipe p = Login2ServicePipe.allLoginPipes.get(remoteHost);
				if (p != null) {
					p.setLastHeartbeat(System.currentTimeMillis());
				}
				setLastHeartbeat(System.currentTimeMillis());
				return super.deal(shb);
			}
			
			@Override
			public boolean deal(ServerStopping ssp) {
				setStopping(true);
				return true;
			}

			@Override
			public boolean deal(UserStatus uss) {
				if (updateUserOnServer(uss, remoteDomain, remotePort)
						&& ClusterConfig.clusterPrimarySynchronizing) {
					for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
							itr.hasNext();) {
						Login2LoginPipe p = (Login2LoginPipe) itr.next();
						UserResult usr = new UserResult();
						if (XYZMixxer.isPrimaryProxyMode()) {
							// Sending out user updates to other primary server, as in proxy
							// mode, mark this user as coming from this server.
							usr.domain = ClusterConfig.clusterServerIP;
							usr.port = ClusterConfig.port;
						} else {
							usr.domain = remoteDomain;
							usr.port = remotePort;
						}
						usr.lastUpdated = uss.time;
						usr.uid = uss.uid;
						usr.status = uss.status;
						p.pipeThrough(usr);
					}
				}
				return true;
			}
	
			/**
			 * @see Login2ServicePipe#deal(UserResult)
			 */
			@Override
			public boolean deal(UserQuery usq) {
				UserResult r = queryUserServer(usq, remoteDomain, remotePort);
				Login2ServicePipe p = Login2ServicePipe.allLoginPipes.get(remoteHost);
				if (p != null) {
					p.pipeThrough(r);
				}
				return true;
			}
			
			@Override
			public void pipeCreated() {
				super.pipeCreated();
				setStopping(false);
				setLastHeartbeat(System.currentTimeMillis());
				if (onlineUsers != null) {
					// If primary-service connection/pipe is lost, reconnecting
					// to service server will get a lot of online users (maybe
					// a hundred thousand users). Synchronizing these users is
					// considered blocking, try to use thread to update all
					// existed users and avoid blocking current thread (NIO).
					XYZMixxer.runTask(new Runnable() {
						
						@Override
						public void run() {
							UserStatus uss = new UserStatus();
							uss.time = time;
							uss.status = IStatus.CONNECTED;
							for (int i = 0; i < onlineUsers.length; i++) {
								int uid = onlineUsers[i];
								if (uid <= 0) {
									continue;
								}
								uss.uid = uid;
								updateUserOnServer(uss, remoteDomain, remotePort);
							}
						}
						
					});

				}
			}
			
			@Override
			public void pipeClosed() {
				super.pipeClosed();
				if (isStopping()) {
					pipeRemoved();
					return;
				}
				if (allService2LoginPipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeLost() {
				super.pipeLost();
				if (isStopping()) {
					pipeRemoved();
					return;
				}
				if (allService2LoginPipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeFailed() {
				super.pipeFailed();
				if (isStopping()) {
					pipeRemoved();
					return;
				}
				if (allService2LoginPipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			protected void pipeRemoved() {
				removeClusterPipeFromMonitor4Client(this);
				allService2LoginPipes.remove(remoteHost);
				// Service server is down or removed from cluster
				allServerStatuses.remove(remoteHost);
				Login2ServicePipe l2sPipe = Login2ServicePipe.allLoginPipes.get(remoteHost);
				if (l2sPipe != null && !l2sPipe.isPipeLive()) {
					Login2ServicePipe removedPipe = Login2ServicePipe.allLoginPipes.remove(remoteHost);
					if (removedPipe != null && removedPipe != l2sPipe) {
						// In rare case, remote service server come back with another pipe,
						// just put pipe back into map.
						Login2ServicePipe.allLoginPipes.put(remoteHost, removedPipe);
					}
				}
				
				if (!isStopping()) {
					// Not being removed intentionally. Maybe service server is crashed or lost.
					// Try to remove server's users and notify other servers about server lost.
					if (XYZMixxer.isPrimaryMasterMode()) {
						ServerStatus ss = new ServerStatus();
						ss.domain = remoteDomain;
						ss.port = remotePort;
						ss.status = IStatus.DISCONNECTED;
						for (Iterator<Login2ServicePipe> itr = Login2ServicePipe.allLoginPipes.values().iterator(); itr.hasNext();) {
							Login2ServicePipe pipe = itr.next();
							if (!remoteHost.equals(pipe.getHost())) {
								// pipe this pipe's another end (service server) to all existed
								// service server
								pipe.pipeThrough(ss);
							}
						}
					}
					XYZMixxer.runTask(new Runnable() {
						
						@Override
						public void run() {
							// Remove all user statuses on the service server which is down
							// Preparing off-line users uses g-zip encoding algorithm, which
							// is considered as CPU-critical
							dropServiceServer(remoteDomain, remotePort);
							
							if (ClusterConfig.clusterPrimarySynchronizing && !XYZMixxer.isPrimaryProxyMode()) {
								ServerStatus ss = new ServerStatus();
								ss.domain = remoteDomain;
								ss.port = remotePort;
								ss.status = IStatus.DISCONNECTED;
								for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
										itr.hasNext();) {
									Login2LoginPipe p = (Login2LoginPipe) itr.next();
									if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
										// ServerStatus event is not coming from the same server of this pipe
										p.pipeThrough(ss);
									}
								}
							} // else: service servers under this primary proxy server won't be exposed to outer world
						}
						
					});
				}
				
				super.pipeRemoved();
			}
			
		};
		p.apiSecret = ClusterConfig.clusterAPISecret;
		p.setLastHeartbeat(System.currentTimeMillis());
		allService2LoginPipes.put(remoteHost, p);
		AbstractClusterPipe.startClusterPipeMonitor4Client(p);
		p.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		SimplePipeRequest.pipe(p);
	}

	/**
	 * Connect other service servers as a service server.
	 * 
	 * @param remoteDomain
	 * @param remotePort
	 */
	public static void createService2ServicePipe(final String remoteDomain, final int remotePort) {
		final String remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
		Service2ServicePipe p = allService2ServicePipes.get(remoteHost);
		if (p != null/* && p.isPipeLive()*/) {
			return; // ignore
		} // else p is null
		p = new Service2ServicePipe() {
	
			@Override
			public String getPipeURL() {
				return "http://" + remoteHost + "/u/p";
			}
	
			@Override
			public String getHttpURL() {
				return "http://" + remoteHost + "/u/r";
			}
	
			@Override
			public void keepPipeLive() {
				setLastHeartbeat(System.currentTimeMillis());
				super.keepPipeLive();
			}
	
			@Override
			public boolean isPipeLive() {
				return super.isPipeLive() && Math.abs(System.currentTimeMillis() - getLastHeartbeat()) < ClusterConfig.clusterPipeHeartBeatTimeoutInterval;
			}
			
			@Override
			public boolean deal(HeartBeat shb) {
				setLastHeartbeat(System.currentTimeMillis());
				return super.deal(shb);
			}
			
			/*
			 * Also see ClusterSerer#pipeEvent
			 */
			private int pipeEvent(ClusterEvent event) {
				int slot = event.receiver & XYZMixxer.SLOTS_MASKS; // event.receiver % XYZMixxer.SLOTS;
				if (ClusterConfig.clusterPrimaryServer) {
					// Primary login server knows which user on which server directly.
					// Select correct pipe and pipe out data event.
					String host = null;
					UserResult r = null;
					synchronized (XYZMixxer.statusMutex[slot]) {
						r = XYZMixxer.allStatuses[slot].get(event.receiver);
						if (r == null || r.status != IStatus.CONNECTED) {
							return 0;
						}
						host = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
					}
					if (!host.equals(remoteHost)) {
						// target server is not the source server of this event 
						boolean sent = false;
						if (!ClusterConfig.clusterPrimaryLoginServer) {
							sent = XYZMixxer.pipeOut(host, event);
							if (!sent // User is not online and unreachable
									&& ClusterConfig.clusterUnreachableUserRemoving && r.lastUpdated > 0
									&& System.currentTimeMillis() - r.lastUpdated > ClusterConfig.clusterMaxUnreachableTime) { 
								ClusterServer.updateUser(event.receiver, false); // update its status on primary server
							}
						} else {
							sent = XYZMixxer.sendPipeOutRPC(host, event);
						}
						return sent ? 1 : 0;
						// return (!ClusterConfig.clusterPrimaryLoginServer
						// 		? XYZMixxer.pipeOut(host, event)
						//				: XYZMixxer.sendPipeOutRPC(host, event)) ? 1 : 0;
					} // else cluster event runs into event loop, ignore proxy this event
					return 0;
				} else {
					// For normal service, try to check cached users. If there are
					// not-expired-yet cached user, try to pipe data through service-
					// service pipe for this cached user.
					String host = null;
					synchronized (XYZMixxer.userMutex[slot]) {
						UserResult r = XYZMixxer.cachedUsers[slot].get(event.receiver);
						if (r != null && (r.lastUpdated == -1 // Logging in from this service server, never expired
								|| Math.abs(System.currentTimeMillis() - r.lastUpdated) < ClusterConfig.clusterUserCachingTime)) {
							if (r.status != IStatus.CONNECTED) {
								return 0;
							}
							host = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
						}
					}
					if (host != null) {
						if (!host.equals(remoteHost)) {
							// target server is not the source server of this event 
							return XYZMixxer.pipeOut(host, event) ? 1 : 0;
						} // else cluster event runs into event loop, ignore proxy this event
						return 0;
					}
					// User is not in cached pool, try to save data event in cross pipe data
					// pool, and then pipe up a query to primary login server. On got query
					// response, data event saved in the pool will be piped out again.
					synchronized (XYZMixxer.dataMutex[slot]) {
						List<ClusterEvent> data = XYZMixxer.crossPipeData[slot].get(event.receiver);
						if (data == null) {
							data = new LinkedList<ClusterEvent>();
							XYZMixxer.crossPipeData[slot].put(event.receiver, data);
						}
						data.add(event);
					}
					UserQuery q = new UserQuery();
					q.uid = event.receiver;
					Service2LoginPipe servicePipe = null;
					synchronized (Service2LoginPipe.pipeMutex) { // across multiple threads
						servicePipe = Service2LoginPipe.servicePipe;
					}
					if (servicePipe != null) {
						servicePipe.pipeThrough(q);
					}
					return -1;
				}
			}

			@Override
			public boolean deal(ClusterEvent evt) {
				if (XYZMixxer.isPrimaryProxyMode()) {
					// Proxy to real service server
					pipeEvent(evt);
				} else {
					ClusterNode serviceNode = ClusterServer.getClusterNode();
					if (serviceNode != null) {
						if (ClusterServer.isUserOnline(evt.receiver)) {
							/*
							 * On receiving event from other cluster server, notify target user on
							 * this cluster node that there is an event.
							 */
							serviceNode.onReceivedEvent(evt);
						} else {
							evt.setSender(0); // Proxy to another service server
							pipeEvent(evt);
						}
					} 
				}
				if (evt.sender > 0 && (!ClusterConfig.clusterPrimaryServer || ClusterConfig.clusterPrimaryProxyServer)) {
					fixQueryResult(evt.sender, remoteDomain, remotePort);
				}
				return true;
			}
			
			@Override
			public void pipeCreated() {
				super.pipeCreated();
				setLastHeartbeat(System.currentTimeMillis());
			}
			
			@Override
			public void pipeClosed() {
				super.pipeClosed();
				if (allService2ServicePipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeLost() {
				super.pipeLost();
				if (allService2ServicePipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
			
			@Override
			public void pipeFailed() {
				super.pipeFailed();
				if (allService2ServicePipes.get(remoteHost) != this) {
					return; // not reconnecting
				}
				delayReconnectPipe(this);
			}
	
			@Override
			protected void pipeRemoved() {
				removeClusterPipeFromMonitor4Client(this);
				allService2ServicePipes.remove(remoteHost);
				Service2ServicePipe.allServicePipes.remove(remoteHost);
				allServerStatuses.remove(remoteHost); // Server disconnected
				super.pipeRemoved();
			}
			
		};
		p.setLastHeartbeat(System.currentTimeMillis());
		allService2ServicePipes.put(remoteHost, p);
		p.apiSecret = ClusterConfig.clusterAPISecret;
		p.port = ClusterConfig.port;
		p.domain = ClusterConfig.clusterServerIP;
		AbstractClusterPipe.startClusterPipeMonitor4Client(p);
		p.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		SimplePipeRequest.pipe(p);
	}

	public static void fixQueryResult(int uid, String remoteDomain, int remotePort) {
		String remoteHost = null;
		int slot = uid & SLOTS_MASKS; // uid % SLOTS;
		long now = System.currentTimeMillis();
		if (isPrimaryProxyMode()) {
			UserStatus ss = new UserStatus();
			ss.status = IStatus.CONNECTED;
			ss.time = now;
			ss.uid = uid;
			if (updateUserOnServer(ss, remoteDomain, remotePort)
					&& ClusterConfig.clusterPrimarySynchronizing) { // Proxy to other servers
				// Synchronize user status to other primary servers
				UserResult usr = new UserResult();
				usr.status = IStatus.CONNECTED;
				usr.uid = uid;
				usr.lastUpdated = now;
				usr.domain = remoteDomain;
				usr.port = remotePort;
				// Events are from other primary server, it should already compromised with primary proxy mode
				for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
						itr.hasNext();) {
					Login2LoginPipe p = (Login2LoginPipe) itr.next();
					if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
						// UserResult event is not coming from the same server of this pipe
						p.pipeThrough(usr);
					}
				}
			}
			return;
		}
		UserResult usr = null;
		synchronized (userMutex[slot]) {
			UserResult r = cachedUsers[slot].get(uid);
			if (r == null || r.status == IStatus.DISCONNECTED
					|| (r.lastUpdated != -1 // -1 means logging in from this server, never expired
					&& Math.abs(now - r.lastUpdated) > ClusterConfig.clusterUserCachingTime)) {
				usr = new UserResult();
				usr.status = IStatus.CONNECTED;
				usr.uid = uid;
				usr.lastUpdated = now;
				usr.domain = remoteDomain;
				usr.port = remotePort;
				cachedUsers[slot].put(uid, usr);
				remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
			} else if (r != null && r.lastUpdated != -1) { // -1 means logging in from this server
				r.lastUpdated = now;
			}
		}
		if (usr != null) {
			ClusterEvent[] events = null;
			// to cross pipe cached events, if any
			synchronized (dataMutex[slot]) {
				List<ClusterEvent> data = crossPipeData[slot].get(usr.uid);
				if (data != null) {
					int size = data.size();
					if (size > 0) {
						events = data.toArray(new ClusterEvent[size]);
						crossPipeData[slot].remove(usr.uid);
					}
				}
			}
			if (events != null && !pipeOut(remoteHost, events)) { // Try to send events
				// if not not sent, user is considered as not online or unreachable
				runNotSentTaks(events);
				// We just update user status. No need to update user status for primary servers
			}
		}
	}

	/**
	 * Got user status on given server.
	 * 
	 * Try to cache or update user status in memory. If there are duplicated
	 * users, notify those servers. If there are servers monitoring these
	 * users, notify those servers.
	 *  
	 * @param uss
	 * @param remoteDomain
	 * @param remotePort
	 */
	public static boolean updateUserOnServer(UserStatus uss, String remoteDomain, int remotePort) {
		UserResult r = null;
		int slot = uss.uid & SLOTS_MASKS; // uss.uid % SLOTS;
		synchronized (statusMutex[slot]) {
			r = allStatuses[slot].get(uss.uid);
			long now = System.currentTimeMillis(); //Math.max(System.currentTimeMillis(), uss.time);
			if (r == null) {
				r = new UserResult();
				r.uid = uss.uid;
				if (uss.status == IStatus.CONNECTED) {
					r.domain = remoteDomain;
					r.port = remotePort;
				} else { // should not reach this branch?
					r.domain = null; // DISCONNECTED
					r.port = -1; // DISCONNECTED
				}
				r.lastUpdated = now;
				r.status = uss.status;
				allStatuses[slot].put(uss.uid, r);
				return true;
			} else {
				boolean updating = false;
				boolean ignoring = false; // for proxy mode, we will ignore updates
				long lastUpdated = r.lastUpdated;
				r.lastUpdated = now;
				if (uss.status == IStatus.CONNECTED) {
					// check duplicated users and notify that server about this update
					if (r.domain != null && r.port != -1 // consider as r.status == CONNECTED
							&& (r.port != remotePort || !r.domain.equals(remoteDomain))) { // Not the same
						if (r.port == ClusterConfig.port && ClusterConfig.clusterServerIP != null
								&& ClusterConfig.clusterServerIP.equals(r.domain)) {
							if (XYZMixxer.isPrimaryProxyMode()) {
								// for primary proxy server, this server won't accept direct connection
								ClusterNode node = ClusterServer.getClusterNode();
								if (node != null) { // gateway
									// notifyDuplicatedLogin(uss.uid, System.currentTimeMillis(),
									//		remoteDomain, remotePort);
									node.onLoginedElsewhere(uss.uid, System.currentTimeMillis(),
											remoteDomain, remotePort);
								}
							} else {
								// old user is on this server
								notifyDuplicatedLogin(uss.uid, System.currentTimeMillis(),
										remoteDomain, remotePort);
							}
							updating = true;
						} else if (ClusterConfig.clusterServerMoving
								&& ClusterConfig.clusterNewServerIP != null
								&& ClusterConfig.clusterNewServerIP.equals(r.domain)
								&& ClusterConfig.clusterNewServerPort == r.port
								&& ClusterConfig.clusterOldServerIP != null
								&& ClusterConfig.clusterOldServerIP.equals(remoteDomain)
								&& ClusterConfig.clusterOldServerPort == remotePort) {
							// updating = false
							ignoring = true;
							//if (ClusterConfig.clusterLogging) {
							//	System.out.println("Cluster: Skip replacing new server status with old server status for " + uss.uid);
							//}
						} else {
							String host = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
							if (XYZMixxer.isPrimaryProxyMode() && remotePort == ClusterConfig.port
									&& ClusterConfig.clusterServerIP != null
									&& ClusterConfig.clusterServerIP.equals(remoteDomain)
									&& Login2ServicePipe.allLoginPipes.containsKey(host)) {
								// The UserStatus object is from SynchronizeUsers RPC.
								// Existed update is from inner service servers, do not update anything
								ignoring = true;
							} else {
								// should pipe through login event!
								Login2ServicePipe p = Login2ServicePipe.allLoginPipes.get(host);
								if (p != null) {
									UserDuplicated usd = new UserDuplicated();
									usd.uid = uss.uid;
									usd.time = System.currentTimeMillis();
									usd.domain = remoteDomain; //r.domain;
									usd.port = remotePort; //r.port;
									p.pipeThrough(usd);
								} else {
									ClusterNode node = ClusterServer.getClusterNode();
									if (node != null && XYZMixxer.isPrimaryProxyMode() && host.equals(node.getNodeID())) {
										// proxy gateway node
										notifyDuplicatedLogin(uss.uid, System.currentTimeMillis(),
												remoteDomain, remotePort);
									} // else just ignore it
								}
								updating = true;
							}
						}
					} else {
						updating = r.status != uss.status;
					}
					if (!ignoring) {
						r.domain = remoteDomain;
						r.port = remotePort;
						r.status = uss.status;
					}
				} else { // disconnected
					if (r.port == remotePort && r.domain != null
							&& r.domain.equals(remoteDomain)) {
						r.domain = null; // DISCONNECTED
						r.port = -1; // DISCONNECTED
						r.status = IStatus.DISCONNECTED;
						updating = true;
					} // else not updating
				}
				if (!updating) {
					return updating; // quick return
				} // else user status updated, need to broadcast to callbacks (servers)
				// Notify those servers which are monitoring on this user.
				String[] callbacks = r.getCallbacks();
				if (callbacks != null) {
					if (Math.abs(now - lastUpdated) > ClusterConfig.clusterUserCachingTime + ClusterConfig.clusterUserCachingTime) {
						r.clearCallbacks();
					} else {
						String remoteHost = null;
						if (remoteDomain != null && remotePort > 0) {
							remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
						}
						int cbSize = callbacks.length;
						for (int i = 0; i < cbSize; i++) {
							String host = callbacks[i];
							if (host == null) {
								continue;
							}
							if (host != null && remoteHost != null && host.equals(remoteHost)) {
								if (ClusterConfig.clusterLogging) {
									System.out.println("Cluster: Skip sending update back to server " + remoteHost);
								}
								// Update is coming from the same host, ignore
								continue;
							}
							Login2ServicePipe p = Login2ServicePipe.allLoginPipes.get(host);
							if (p != null) {
								if (Login2ServicePipe.allLoginPipes.containsKey(remoteHost)) {
									// inner service servers
									p.pipeThrough(r);
								} else {
									if (XYZMixxer.isPrimaryProxyMode()) {
										UserResult usr = new UserResult();
										usr.uid = r.uid;
										usr.status = r.status;
										usr.lastUpdated = r.lastUpdated;
										usr.domain = ClusterConfig.clusterServerIP;
										usr.port = ClusterConfig.port;
										p.pipeThrough(usr);
									} else {
										p.pipeThrough(r);
									}
								}
							}
						}
					}
				} // end of if callbacks
				
				return updating;
			}
		} // end of statusMutex 
	}

	public static UserResult queryUserServer(UserQuery usq, String remoteDomain, int remotePort) {
		String remoteHost = remotePort == 80 ? remoteDomain : (remoteDomain + ":" + remotePort);
		UserResult r = null;
		int slot = usq.uid & SLOTS_MASKS; // usq.uid % SLOTS;
		synchronized (statusMutex[slot]) {
			r = allStatuses[slot].get(usq.uid);
			if (r == null) {
				r = new UserResult();
				r.domain = null;
				r.port = -1;
				r.uid = usq.uid;
				r.status = IStatus.DISCONNECTED;
				allStatuses[slot].put(usq.uid, r);
			}
			r.lastUpdated = System.currentTimeMillis();
			r.addCallback(remoteHost);
		}
		if (XYZMixxer.isPrimaryProxyMode() && r.domain != null && r.port > 0) {
			// UserQuery is from inner service servers of primary proxy server
			String host = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
			boolean proxying = !Login2ServicePipe.allLoginPipes.containsKey(host);
			// User is not on inner service servers, it is from outer world.
			if (!proxying) {
				ClusterNode node = ClusterServer.getClusterNode();
				proxying = node != null && host.equals(node.getNodeID());
				// User is on gateway server
			}
			if (proxying) {
				// Respond query with result pointing to this proxy server.
				int status = r.status;
				r = new UserResult();
				r.domain = ClusterConfig.clusterServerIP;
				r.port = ClusterConfig.port;
				r.uid = usq.uid;
				r.status = status;
			}
		}
		return r;
	}

	public static boolean sendPipeOutRPC(final String remoteHost, final ClusterEvent... events) {
//		if (ClusterConfig.port == usr.port && usr.domain.equals(ClusterConfig.clusterServerIP)) {
//			System.out.println("Try to send out RPC to this server?!");
//			return false;
//		}
		if (events == null || events.length == 0) {
			return false;
		}
		SendEvent r = new SendEvent() {
	
			@Override
			public String getHttpURL() {
				return "http://" + remoteHost + "/u/r";
			}
	
			@Override
			public void ajaxOut() {
				if (returnCode != OK) {
					onFallback();
				}
			}
			
			@Override
			public void ajaxFail() {
				if (!allService2LoginPipes.containsKey(remoteHost)) {
					// Target service has been shutdown!
					return; // ignore
				}
				retryOrFallback();
			}
			
			@Override
			protected void onFallback() {
				pipeOut(remoteHost, events);
			}
			
		};
		if (ClusterConfig.clusterPrimaryLoginServer) {
			for (int i = 0; i < events.length; i++) {
				if (events[i] != null) {
					events[i].setSender(0);
				}
			}
		}
		r.events = events;
		r.serverIP = ClusterConfig.clusterServerIP;
		r.serverPort = ClusterConfig.port;
		r.apiSecret = ClusterConfig.clusterAPISecret;
		r.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		SimpleRPCRequest.request(r);
		return true;
	}

	public static void gotUserOnServer(UserResult usr) {
		int slot = usr.uid & SLOTS_MASKS; // usr.uid % SLOTS;
		synchronized (userMutex[slot]) {
			UserResult r = cachedUsers[slot].get(usr.uid);
			if (r != null && r.lastUpdated == -1
					&& r.port == ClusterConfig.port && ClusterConfig.clusterServerIP != null
					&& ClusterConfig.clusterServerIP.equals(r.domain)) {
				usr.lastUpdated = -1; // Logging from this server, never expired
			}
			cachedUsers[slot].put(usr.uid, usr);
		}
		boolean sent = false;
		String remoteHost = null;
		ClusterEvent[] events = null;
		// to cross pipe cached events, if any
		synchronized (dataMutex[slot]) {
			List<ClusterEvent> data = crossPipeData[slot].get(usr.uid);
			if (data != null) {
				int size = data.size();
				if (size > 0) {
					if (usr.domain != null && usr.port > 0) {
						remoteHost = usr.port == 80 ? usr.domain : (usr.domain + ":" + usr.port);
					}
					events = data.toArray(new ClusterEvent[size]);
					crossPipeData[slot].remove(usr.uid);
				}
			}
		}
		if (events != null) {
			if (remoteHost != null) {
				sent = pipeOut(remoteHost, events);
				// if not sent, as it is invoked from Login2ServicePipe, there is no needs to
				// update user status in primary status servers
			}
			if (!sent) {
				// For those events which are not sent, try to run failed call back.
				runNotSentTaks(events);
			}
		}
	}

	private static void runNotSentTaks(ClusterEvent[] events) {
		if (events.length == 1) {
			ClusterEvent e = events[0];
			Runnable cb = e.getCallback();
			if (cb != null) {
				XYZMixxer.runTask(cb);
			}
		} else {
			final List<Runnable> cbs = new ArrayList<Runnable>(events.length);
			for (int i = 0; i < events.length; i++) {
				ClusterEvent e = events[i];
				Runnable cb = e.getCallback();
				if (cb != null) {
					cbs.add(cb);
				}
			}
			int cbSize = cbs.size();
			if (cbSize == 1) {
				XYZMixxer.runTask(cbs.get(0));
			} else if (cbSize > 0) {
				XYZMixxer.runTask(new Runnable() {
					@Override
					public void run() {
						for (Iterator<Runnable> itr = cbs.iterator(); itr.hasNext();) {
							Runnable r = (Runnable) itr.next();
							try {
								r.run();
							} catch (Throwable e) {
								e.printStackTrace();
							}
						}
					}
				});
			}
		}
	}

	/**
	 * Notify that user is login elsewhere.
	 * 
	 * @param uid
	 * @param time
	 * @param host
	 * @param port
	 */
	public static void notifyDuplicatedLogin(int uid, long time, String host, int port) {
		if (!ClusterConfig.clusterPrimaryServer) {
			// Update cached user status
			int slot = uid & SLOTS_MASKS; // uid % SLOTS;
			synchronized (XYZMixxer.userMutex[slot]) {
				UserResult r = XYZMixxer.cachedUsers[slot].get(uid);
				if (r == null) {
					r = new UserResult();
					cachedUsers[slot].put(uid, r);
				}
				r.status = IStatus.CONNECTED;
				r.uid = uid;
				r.lastUpdated = time;
				r.domain = host;
				r.port = port;
			}
		}
		ClusterNode node = ClusterServer.getClusterNode();
		if (node != null) {
			// Mark pipe as kicked and send out notification to client, close
			// pipe later.
			node.onLoginedElsewhere(uid, time, host, port);
		}
	}

	/**
	 * Return all known service servers, excluding given host.
	 * If specified, service servers from other primary login server are not
	 * included. Or return servers includes all servers.
	 * For primary login server side only.
	 * 
	 * @param filteredHost
	 * @param includeAllServers
	 * @return
	 */
	public static Map<String, ServerStatus> getAllServiceServers(String filteredHost, boolean includeAllServers) {
		// Generate online servers from Login2ServicePipe#allLoginPipes
		Map<String, ServerStatus> servers = new HashMap<String, ServerStatus>();
		for (Iterator<Login2ServicePipe> itr = Login2ServicePipe.allLoginPipes.values().iterator(); itr.hasNext();) {
			Login2ServicePipe pipe = itr.next();
			if (!filteredHost.equals(pipe.getHost())) {
				ServerStatus ss = new ServerStatus();
				ss.domain = pipe.getRemoteDomain();
				ss.port = pipe.port;
				if (Math.abs(System.currentTimeMillis() - pipe.getLastHeartbeat()) > ClusterConfig.clusterPipeHeartBeatTimeoutInterval) {
					// Server is down! Notify remote server that it's down!
					ss.status = IStatus.DISCONNECTED;
				} else {
					ss.status = IStatus.CONNECTED;
				}
				servers.put(pipe.getHost(), ss);
			}
		}
		if (includeAllServers) {
			// XYZMixxer.allServerStatuses may contains service servers from other primary/login servers
			for (Iterator<Entry<String, ServerStatus>> itr = XYZMixxer.allServerStatuses.entrySet().iterator(); itr.hasNext();) {
				Entry<String, ServerStatus> entry = (Entry<String, ServerStatus>) itr.next();
				String remoteHost = entry.getKey();
				if (!servers.containsKey(remoteHost)) {
					servers.put(remoteHost, entry.getValue());
				}
			}
		}
		if (!ClusterConfig.clusterPrimaryLoginServer) {
			String host = ClusterConfig.port == 80 ? ClusterConfig.clusterServerIP
					: (ClusterConfig.clusterServerIP + ":" + ClusterConfig.port);
			if (!servers.containsKey(host)) {
				ServerStatus ps = new ServerStatus();
				ps.domain = ClusterConfig.clusterServerIP;
				ps.port = ClusterConfig.port;
				ps.status = IStatus.CONNECTED;
				servers.put(host, ps);
			}
		}
		return servers;
	}

	/*
	 * Service server is down, clear up and notify primary login servers.
	 */
	private static void dropServiceServer(String remoteDomain, int remotePort) {
		for (int i = 0; i < SLOTS; i++) {
			List<UserResult> toRemoved = new LinkedList<UserResult>();
			int removedSize = 0;
			synchronized (statusMutex[i]) {
				Map<Object, UserResult> ass = allStatuses[i];
				for (Iterator<UserResult> itr = ass.values().iterator(); itr.hasNext();) {
					UserResult usr = (UserResult) itr.next();
					if (usr.port == remotePort && remoteDomain.equals(usr.domain)) {
						toRemoved.add(usr);
						removedSize++;
						itr.remove();
					}
				}
			} // end of synchronized block of statusMutex
			if (ClusterConfig.clusterPrimarySynchronizing && removedSize > 0) {
				// Check whether we need synchronization
				boolean needPiping = false;
				for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
						itr.hasNext();) {
					Login2LoginPipe p = (Login2LoginPipe) itr.next();
					if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
						// not coming from the same server of this pipe
						needPiping = true;
						break;
					}
				}
				if (needPiping) {
					// prepare ServerLost event
					int slot = i;
					int maxUserID = slot;
					byte[] bits = new byte[(ClusterConfig.clusterMaxUsers >> (XYZMixxer.SLOTS_BITS + 3)) + 1];
					boolean hasUsers = false;
					for (Iterator<UserResult> iter = toRemoved.iterator(); iter.hasNext();) {
						UserResult usr = (UserResult) iter.next();
						if (usr != null && usr.status == IStatus.CONNECTED) {
							// Only return online users
							int uid = usr.uid;
							if (uid > maxUserID) {
								maxUserID = uid;
							}
							int location = (uid - slot) >> XYZMixxer.SLOTS_BITS;
							int index = location >> 3;
							int bit = location & 0x7;
							bits[index] |= 1 << bit;
							
							hasUsers = true;
						}
					} // end of for loop
					if (hasUsers) {
						ServerLost sl = new ServerLost();
						// Specify the server detects that service server is lost
						sl.sourceDomain = ClusterConfig.clusterServerIP;
						sl.sourcePort = ClusterConfig.port;
						
						// Specify which service server is lost 
						if (XYZMixxer.isPrimaryProxyMode()) {
							sl.domain = ClusterConfig.clusterServerIP;
							sl.port = ClusterConfig.port;
						} else {
							sl.domain = remoteDomain;
							sl.port = remotePort;
						}
						sl.time = System.currentTimeMillis();
						sl.slot = slot;
						
						int size = ((maxUserID - slot) >> (XYZMixxer.SLOTS_BITS + 3)) + 1;
						//*
						// Make offline bits more compact
						ByteArrayOutputStream out = new ByteArrayOutputStream();
						GZIPOutputStream gZipOut = null;
						try {
							gZipOut = new GZIPOutputStream(out);
							gZipOut.write(bits, 0, size);
						} catch (IOException e) {
							e.printStackTrace();
						} finally {
							if (gZipOut != null) {
								try {
									gZipOut.close();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
						sl.offlineBits = out.toByteArray();
						// */
						/*
						// Raw data without gzip encoding
						sl.onlineBits = new byte[size];
						System.arraycopy(bits, 0, sl.onlineBits, 0, size);
						// */
						for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
								itr.hasNext();) {
							Login2LoginPipe p = (Login2LoginPipe) itr.next();
							if (remotePort != p.port || !remoteDomain.equals(p.domain)) {
								// not coming from the same server of this pipe
								p.pipeThrough(sl);
							}
						}
					}
				} // end of needPiping
			} // end of #clusterPrimarySynchronizing 
		}
	}

	private static void runTask(Runnable r) {
		if (executor != null) {
			try {
				executor.execute(r);
				return;
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		try {
			Thread thread = new Thread(r, "Cluster Event Callback Worker");
			thread.setDaemon(true);
			thread.start();
		} catch (Throwable e) {
			// Thread may not start if there are too many threads started.
			e.printStackTrace();
		}
	}
	
}
