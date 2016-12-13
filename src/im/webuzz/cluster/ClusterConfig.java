package im.webuzz.cluster;

import im.webuzz.cluster.xyz.XYZMixxer;
import im.webuzz.threadpool.ThreadPoolExecutorConfig;

import java.util.Properties;

public class ClusterConfig {
	
	//public static final String configKeyPrefix = "cluster";

	/**
	 * Server port. Default port is 80.
	 * Server might need to be run by root user to access port 80 or other
	 * port which is less 1024.  
	 */
	public static int port = 80;

	/**
	 * Whether this is primary server (login server) or not.
	 * Primary server will collect all user login statuses and all service server
	 * information.
	 */
	public static boolean clusterPrimaryServer = true;
	/**
	 * Whether primary server also perform as login server only or not.
	 * 
	 * In low traffic mode, primary server is performing as both login server
	 * and service server. In this case, primaryLoginServer will be false.
	 * 
	 * If primaryLoginServer is true, primary server won't perform as a service
	 * server.
	 */
	public static boolean clusterPrimaryLoginServer = false;

	/**
	 * Cluster Server IP. Usually private IP for Intranet is very fast.
	 */
	public static String clusterServerIP = "127.0.0.1";

	/**
	 * All cluster server IPs, including private IPs or public IPs.
	 */
	public static String[] clusterAllServerIPs = null;

	public static int clusterBackupServerStrategy = 1; // 0 : random, 1: order looping (default prefix -> backend server list)
	
	public static int clusterBackupServerRandomFactor = 5; // For random mode, 4 URLs will have maximum 4 * 5 random checks 

	public static int clusterMinReconnectRetries = 3; // If less, try to retries on default URL prefix for order looping
	
	public static long clusterMaxReconnectInterval = 3000; // 3s If pipe to primary can not be setup in less than 3s, remote server is considered as down  

	/**
	 * Primary server for other non-primary cluster server to connect.
	 */
	public static String clusterGlobalServer = "http://127.0.0.1";

	/**
	 * Backup servers for primary server. In cases primary server is down,
	 * non-primary cluster (service server) can connect to these primary
	 * servers.
	 */
	public static String[] clusterGlobalBackupServers = null;

	/**
	 * Gateway server for other servers to send events into cluster.
	 * @see ClusterSendEvent
	 */
	public static String clusterGatewayServer = "http://127.0.0.1";

	/**
	 * All gateway servers for other servers to send events into cluster.
	 * In case the above cluster gate server is down, try gateway servers
	 * listed in this variable.
	 * @see ClusterSendEvent
	 */
	public static String[] clusterAllGatewayServers = null;

	/**
	 * Server IP of master primary/login server.
	 * If set to other server, this server (primary server) will try to connect
	 * to that server and synchronize server statuses and user statuses.
	 */
	public static String clusterMasterServerIP = null;
	
	/**
	 * Server port of master primary/login server.
	 */
	public static int clusterMasterServerPort = 80;
	
	/**
	 * Whether primary login server supports synchronizing statuses to slave
	 * login server or not.
	 * Normally, this value should be true. If performance is considered and
	 * only one primary login server is running, set this value to false will
	 * improve performance.
	 */
	public static boolean clusterPrimarySynchronizing = true;
	
	/**
	 * Whether this primary/login server is performing as proxy server or not.
	 * If this primary server is in proxy mode, it won't expose service servers
	 * that is connecting to it to other service servers. That is to say, this
	 * primary/login server (also a service server) is known to other servers
	 * that it is holding all users that is connecting to its sub-service servers.
	 * Other service servers do not know any inside details.
	 */
	public static boolean clusterPrimaryProxyServer = false; 
	
	/**
	 * For primary server, if transparent is true, server won't know the details
	 * of cluster events. Only service server need to know about all cluster
	 * event details.
	 */
	public static boolean clusterEventTransparent = false;
	
	/**
	 * If gateway server or network connection are down, try to send events again.
	 * 
	 * If value is less or equal than 0, it is not supporting re-sending events.
	 * 
	 * For client side @see ClusterServer#sendClusterEvent
	 */
	public static int clusterMaxEventResendings = 3;

	/**
	 * API secret to avoid being sent with flood of unauthorized events.
	 * Client side will set this value to field #apiSecret on every cluster RPC.
	 */
	public static String clusterAPISecret = null;

	/**
	 * All API secrets for different accounts.
	 * These secrets can be used to update primary cluster API secrets. 
	 */
	public static String[] clusterAllAPISecrets = null;

	/**
	 * Gateway secret to avoid being sent with flood of unauthorized events
	 * through gateway. 
	 * 
	 * Gateway secret may be different from API secret. Cluster API
	 * secret is considered as cluster internal secret, and should never be
	 * exposed to outside world. While gateway secret may provide to third-
	 * parties for developments.
	 * 
	 * Client side will set this value to field #apiSecret on every gateway RPC.
	 */
	public static String clusterGatewaySecret = null;

	/**
	 * All gateway secrets for different accounts through gateway.
	 * This secrets can used to update primary cluster gateway secret. 
	 */
	public static String[] clusterAllGatewaySecrets = null;
	
	public static ThreadPoolExecutorConfig clusterWorkerPool = new ThreadPoolExecutorConfig();
	
	static {
		/**
		 * Core thread number. Core threads will be kept in thread pool to
		 * make server more responsible.
		 */
		clusterWorkerPool.coreThreads = 20;

		/**
		 * Max thread number. Server will allow this number of threads at the
		 * peak. Default to 128. If set to -1, if there is no limit.
		 */
		clusterWorkerPool.maxThreads = 128;

		/**
		 * Idle thread number. Server will keep this number of idle threads if
		 * possible. Default is 10.
		 */
		clusterWorkerPool.idleThreads = 10;
		
		/**
		 * If a thread is idle for given seconds, and thread number is greater
		 * than max threads, this thread will be recycled.
		 */
		clusterWorkerPool.threadIdleSeconds = 120L;

		/**
		 * Queue task number. Server will keep this number of tasks waiting in
		 * Queue. Default is 10.
		 */
		clusterWorkerPool.queueTasks = 10;
		
		/**
		 * Allow pooled threads to time out or not.
		 */
		clusterWorkerPool.threadTimeout = false;
	}

	/**
	 * Initial capacity of all maps.
	 */
	public static int clusterInitialCapacity = 10000;

	/**
	 * The time that remote server cache user status for, 3 minutes by default
	 */
	public static long clusterUserCachingTime = 3 * 60000;

	/**
	 * Supports removing not reachable user or not. In case cluster node
	 * crashes and user is off-line, user status may be kept as online in
	 * primary status servers.
	 */
	public static boolean clusterUnreachableUserRemoving = true;
	
	/**
	 * The maximum time that user is being in not reachable status. If time
	 * exceeds that maximum time, update user status in primary servers. 
	 */
	public static long clusterMaxUnreachableTime = 12L * 3600 * 1000;
	
	/**
	 * If there are no heart beats for given interval, mark pipe as not alive.
	 * 1 minutes by default.
	 */
	public static long clusterPipeHeartBeatTimeoutInterval = 60000;
	
	/**
	 * The minimum reconnecting retries before removing pipe connections.
	 * If server's time changes suddenly, pipes may be treated as expired and
	 * server should try to reconnect pipes. But if time changes a lot, and
	 * server may think long time has been past since remote server is last
	 * seen and remove remote server from list and then cluster reconnecting
	 * is broken.
	 * To avoid the above scenario happening, we need to make sure that pipes
	 * have tried to reconnect remote server for given minimum times.
	 */
	public static int clusterReconnectMinimumRetries = 20;
	
	/**
	 * The maximum time that cluster will try to reconnection other servers,
	 * 2 minutes by default. 
	 */
	public static long clusterReconnectMaxTime = 2 * 60000;
	
	/**
	 * delay += incremental interval + delay * incremental rate;
	 */
	public static long clusterReconnectIncrementalInterval = 20;
	public static double clusterReconnectIncrementalRate = 0.2;
	/**
	 * retries++;
	 * delay = fixed interval + per retry interval * retries;
	 */
	public static long clusterReconnectFixedInterval = 2000;
	public static long clusterReconnectFixedPerRetryInterval = 50;
	/**
	 * if (delay > max interval) delay = maxInerval;
	 */
	public static long clusterReconnectMaxInterval = 10000;

	/**
	 * Interval of server pipe heart beat.
	 * 10s by default.
	 */
	public static long clusterServerHeartBeatInterval = 10000;
	
	/**
	 * Interval of monitoring pipe.
	 * 1s by default.
	 */
	public static long clusterPipeMonitorInterval = 1000;
	
	/**
	 * Initial and incremental size of call-backs array.
	 */
	public static int clusterUserQueryCallbackSize = 3;
	
	/**
	 * Maximum users for the cluster to hold.
	 * This value is used for estimation for primary/login server data synchronization.
	 * This value should be increased on user count increases.
	 */
	public static int clusterMaxUsers = 100000000;
	
	/**
	 * Maximum initialize time for cluster server to be online or joined together.
	 */
	public static long clusterMaxInitializeTime = 60000;
	
	public static boolean clusterServerMoving = false;
	
	public static String clusterOldServerIP = null;
	
	public static int clusterOldServerPort = 80;
	
	public static String clusterNewServerIP = null;
	
	public static int clusterNewServerPort = 80;
	
	public static boolean clusterLogging = true;
	
	public static void update(Properties props) {
		if (clusterPrimaryServer) {
			/*
			 * Try to start, stop or switch synchronizing pipe
			 */
			XYZMixxer.updatePrimaryMasterServer(clusterMasterServerIP, clusterMasterServerPort);
		}
		// What about primary server with login only?
		XYZMixxer.updatePrimaryGlobalServer(clusterGlobalServer, clusterGlobalBackupServers);
		XYZMixxer.updatePoolConfigurations();
	}
	
}
