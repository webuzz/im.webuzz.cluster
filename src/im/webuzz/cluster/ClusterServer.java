package im.webuzz.cluster;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import im.webuzz.cluster.events.IStatus;
import im.webuzz.cluster.events.UserQuery;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.events.UserStatus;
import net.sf.j2s.ajax.SimpleFactory;
import net.sf.j2s.ajax.SimplePipeHelper;
import net.sf.j2s.ajax.SimplePipeRunnable;
import net.sf.j2s.ajax.SimpleRPCRequest;
import net.sf.j2s.ajax.SimpleSerializable;
import im.webuzz.cluster.pipes.Login2LoginPipe;
import im.webuzz.cluster.pipes.Service2LoginPipe;
import im.webuzz.cluster.xyz.XYZMixxer;
import im.webuzz.threadpool.SimpleThreadPool;

/**
 * Cluster server, providing pipeTo, pipeUp API for piping data across
 * cluster servers.
 * 
 * Direction:
 * down: login server to service server.
 * up: service server to login server.
 * to: service server to service server
 * 
 * @author zhourenjian
 *
 */
public class ClusterServer {
	
	private static ClusterNode serviceNode;
	
	private static boolean initialized = false;
	
	/**
	 * Whether this server is cluster or not.
	 * 
	 * Once {@link ClusterServer#initialize(ClusterNode)} is called, it is
	 * considered as a cluster server.
	 * 
	 * @return
	 */
	public static boolean isClusterServer() {
		return initialized;
	}
	
	/**
	 * Initialize cluster server and bind a cluster node to cluster server
	 * to provide services, or it will be just a cluster gateway.
	 * 
	 * @param node
	 */
	public static void initialize(ClusterNode node) {
		if (initialized) {
			return;
		}
		initialized = true;
		serviceNode = node;
		
		if (ClusterConfig.clusterEventTransparent) {
			SimpleSerializable.fallbackFactory = new SimpleFactory() {
				
				@Override
				public SimpleSerializable createInstance() {
					return new ClusterEvent();
				}
				
			};
		}
		
		XYZMixxer.initialize();
		SimpleThreadPool.initializePool();

		if (ClusterConfig.clusterPrimaryServer) {
			// For primary server, it must be a login server, there is no needs
			// to connect to itself.
			XYZMixxer.updatePrimaryMasterServer(ClusterConfig.clusterMasterServerIP, ClusterConfig.clusterMasterServerPort);
			return;
		}
		
		XYZMixxer.updatePrimaryGlobalServer(ClusterConfig.clusterGlobalServer, ClusterConfig.clusterGlobalBackupServers);

		Service2LoginPipe servicePipe = new Service2LoginPipe();
		servicePipe.setPipeHelper(new SimplePipeHelper.IPipeThrough() {
			
			public void helpThrough(SimplePipeRunnable pipe,
					SimpleSerializable[] objs) {
				SimplePipeHelper.pipeIn(pipe.pipeKey, objs);
			}
		
		});
		servicePipe.apiSecret = ClusterConfig.clusterAPISecret;
		servicePipe.ajaxRun();
		Service2LoginPipe.servicePipe = servicePipe;

		XYZMixxer.createLogin2ServicePipe();
	}
	
	/**
	 * Get cluster node connected to this cluster server.
	 * 
	 * @return
	 */
	public static ClusterNode getClusterNode() {
		return serviceNode;
	}

	/**
	 * Detect whether a user is online or not.
	 * For primary server, status will be detected from all status map.
	 * For service server, status will be detected from cached user map. Only
	 * users logging in this server will be returned as online.
	 * 
	 * @param uid
	 * @return online or not
	 */
	public static boolean isUserOnline(int uid) {
		int slot = uid & XYZMixxer.SLOTS_MASKS; // uid % XYZMixxer.SLOTS;
		if (ClusterConfig.clusterPrimaryServer) {
			synchronized (XYZMixxer.statusMutex[slot]) {
				UserResult r = XYZMixxer.allStatuses[slot].get(uid);
				if (r == null) {
					return false;
				}
				return r.status == IStatus.CONNECTED;
			}
		} else {
			synchronized (XYZMixxer.userMutex[slot]) {
				UserResult r = XYZMixxer.cachedUsers[slot].get(uid);
				if (r == null) {
					return false;
				}
				return r.status == IStatus.CONNECTED && r.lastUpdated == -1; // -1, logging in from this server, never expired
			}
		}
	}

	public static int getUserClusterStatus(int uid) {
		int slot = uid & XYZMixxer.SLOTS_MASKS; // uid % XYZMixxer.SLOTS;
		if (ClusterConfig.clusterPrimaryServer) {
			synchronized (XYZMixxer.statusMutex[slot]) {
				UserResult r = XYZMixxer.allStatuses[slot].get(uid);
				if (r != null && r.status == IStatus.CONNECTED) {
					if (r.port == ClusterConfig.port && ClusterConfig.clusterServerIP != null
							&& ClusterConfig.clusterServerIP.equals(r.domain)) {
						// on this primary server
						return 1; // here
					} else {
						return -1; // else where
					}
				} else {
					return 0; // Off-line
				}
			}
		} else {
			synchronized (XYZMixxer.userMutex[slot]) {
				UserResult r = XYZMixxer.cachedUsers[slot].get(uid);
				if (r != null && r.status == IStatus.CONNECTED
						&& (r.lastUpdated == -1 // Logging in from this server, never expired
						|| Math.abs(System.currentTimeMillis() - r.lastUpdated) < ClusterConfig.clusterUserCachingTime)) {
					if (r.port == ClusterConfig.port && ClusterConfig.clusterServerIP != null
							&& ClusterConfig.clusterServerIP.equals(r.domain)) {
						// on this server
						return 1; // here
					} else {
						return -1; // else where
					}
				} else {
					return 0; // not known yet
				}
			}
		}
	}
	

	/**
	 * Query specified user status across cluster servers.
	 * 
	 * @param event
	 * @return
	 */
	public static int queryUserClusterStatus(int uid) {
		int slot = uid & XYZMixxer.SLOTS_MASKS; // uid % XYZMixxer.SLOTS;
		if (ClusterConfig.clusterPrimaryServer) {
			// Primary login server knows which user on which server directly.
			// Select correct pipe and pipe out data event.
			synchronized (XYZMixxer.statusMutex[slot]) {
				UserResult r = XYZMixxer.allStatuses[slot].get(uid);
				if (r == null || r.status != IStatus.CONNECTED) {
					return 0;
				}
			}
			return 1;
		} else {
			// For normal service, try to check cached users. If there are
			// not-expired-yet cached user, try to pipe data through service-
			// service pipe for this cached user.
			synchronized (XYZMixxer.userMutex[slot]) {
				UserResult r = XYZMixxer.cachedUsers[slot].get(uid);
				if (r != null && (r.lastUpdated == -1
						|| Math.abs(System.currentTimeMillis() - r.lastUpdated) < ClusterConfig.clusterUserCachingTime)) {
					if (r.status != IStatus.CONNECTED) {
						return 0;
					}
					return 1;
				}
			}
			UserQuery q = new UserQuery();
			q.uid = uid;
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

	/**
	 * Pipe up user status on signed in server to primary login server.
	 * 
	 * @param uid
	 * @param online
	 */
	public static void updateUser(int uid, boolean online) {
		updateUser(uid, online, ClusterConfig.clusterServerIP, ClusterConfig.port);
	}
	
	/**
	 * Pipe up user status on signed in server to primary login server.
	 * 
	 * @param uid
	 * @param online
	 */
	public static void updateUser(int uid, boolean online, String serverDomain, int serverPort) {
		UserStatus uss = new UserStatus();
		uss.uid = uid;
		uss.time = System.currentTimeMillis();
		uss.status = online ? IStatus.CONNECTED : IStatus.DISCONNECTED;
		if (ClusterConfig.clusterPrimaryServer) {
			if (XYZMixxer.updateUserOnServer(uss, serverDomain, serverPort)
					&& ClusterConfig.clusterPrimarySynchronizing) {
				// Synchronize user status to other primary servers
				UserResult usr = new UserResult();
				if (XYZMixxer.isPrimaryProxyMode()) {
					usr.domain = ClusterConfig.clusterServerIP;
					usr.port = ClusterConfig.port;
				} else {
					usr.domain = serverDomain;
					usr.port = serverPort;
				}
				usr.lastUpdated = uss.time;
				usr.uid = uid;
				usr.status = uss.status;
				for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator();
						itr.hasNext();) {
					Login2LoginPipe p = (Login2LoginPipe) itr.next();
					p.pipeThrough(usr);
				}
			}
		} else {
			int slot = uid & XYZMixxer.SLOTS_MASKS; // uid % XYZMixxer.SLOTS;
			UserResult r = null;
			if (online) {
				r = new UserResult();
				r.uid = uss.uid;
				r.domain = serverDomain;
				r.port = serverPort;
				r.lastUpdated = -1; // Logging in from this service server, never expired
				r.status = uss.status;
			}
			synchronized (XYZMixxer.userMutex[slot]) {
				if (!online) {
					XYZMixxer.cachedUsers[slot].remove(uid); // clear local cache
				} else {
					XYZMixxer.cachedUsers[slot].put(uss.uid, r);
				}
			}
			Service2LoginPipe servicePipe = null;
			synchronized (Service2LoginPipe.pipeMutex) { // across multiple threads
				servicePipe = Service2LoginPipe.servicePipe;
			}
			if (servicePipe != null) {
				servicePipe.pipeThrough(uss);
			}
		}
	}

	/**
	 * Pipe event across cluster servers to specified user.
	 * 
	 * @param event
	 * @return
	 */
	public static int pipeEvent(ClusterEvent event) {
		int slot = event.receiver & XYZMixxer.SLOTS_MASKS; // event.receiver % XYZMixxer.SLOTS;
		if (ClusterConfig.clusterPrimaryServer) {
			// Primary login server knows which user on which server directly.
			// Select correct pipe and pipe out data event.
			String remoteHost = null;
			UserResult r = null;
			synchronized (XYZMixxer.statusMutex[slot]) {
				r = XYZMixxer.allStatuses[slot].get(event.receiver);
				if (r == null || r.status != IStatus.CONNECTED) {
					return 0;
				}
				remoteHost = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
			}
			if (ClusterConfig.clusterServerMoving
					&& ClusterConfig.clusterOldServerIP != null
					&& ClusterConfig.clusterOldServerIP.equals(r.domain)
					&& ClusterConfig.clusterOldServerPort == r.port) {
				if (ClusterConfig.clusterNewServerIP != null && ClusterConfig.clusterNewServerIP.length() > 0
						&& ClusterConfig.clusterNewServerPort > 0) {
					updateUser(event.receiver, true, ClusterConfig.clusterNewServerIP, ClusterConfig.clusterNewServerPort);
				} else {
					updateUser(event.receiver, true);
				}
				synchronized (XYZMixxer.statusMutex[slot]) {
					r = XYZMixxer.allStatuses[slot].get(event.receiver);
					if (r == null || r.status != IStatus.CONNECTED) {
						return 0;
					}
					remoteHost = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
				}
			}
			boolean sent = false;
			if (!ClusterConfig.clusterPrimaryLoginServer) {
				sent = XYZMixxer.pipeOut(remoteHost, event);
				if (!sent // User is not online or unreachable
						&& ClusterConfig.clusterUnreachableUserRemoving && r.lastUpdated > 0
						&& System.currentTimeMillis() - r.lastUpdated > ClusterConfig.clusterMaxUnreachableTime) { 
					updateUser(event.receiver, false); // update its status on primary server
				}
			} else {
				sent = XYZMixxer.sendPipeOutRPC(remoteHost, event);
			}
			return sent ? 1 : 0;
			// return (!ClusterConfig.clusterPrimaryLoginServer
			// 			? XYZMixxer.pipeOut(remoteHost, event)
			//					: XYZMixxer.sendPipeOutRPC(remoteHost, event)) ? 1 : 0;
		} else {
			// For normal service, try to check cached users. If there are
			// not-expired-yet cached user, try to pipe data through service-
			// service pipe for this cached user.
			String remoteHost = null;
			synchronized (XYZMixxer.userMutex[slot]) {
				UserResult r = XYZMixxer.cachedUsers[slot].get(event.receiver);
				if (r != null && (r.lastUpdated == -1 // Logging in from this service server, never expired
						|| Math.abs(System.currentTimeMillis() - r.lastUpdated) < ClusterConfig.clusterUserCachingTime)) {
					if (r.status != IStatus.CONNECTED) {
						return 0;
					}
					remoteHost = r.port == 80 ? r.domain : (r.domain + ":" + r.port);
				}
			}
			if (remoteHost != null) {
				return XYZMixxer.pipeOut(remoteHost, event) ? 1 : 0;
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
	
	/**
	 * Send ClusterSendEvent request to gateway server. Add API secret to the
	 * request to authorize this request in cluster servers.
	 * 
	 * @param r
	 */
	public static void requestSendEvents(ClusterSendEvent r) {
		r.apiSecret = ClusterConfig.clusterAPISecret;
		r.clientMode = true;
		r.setSimpleVersion(SimpleSerializable.LATEST_SIMPLE_VERSION);
		SimpleRPCRequest.request(r);
	}
	
}
