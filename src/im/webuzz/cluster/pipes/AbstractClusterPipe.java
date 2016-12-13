package im.webuzz.cluster.pipes;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.calls.ClosePipe;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.xyz.XYZMixxer;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import net.sf.j2s.ajax.ISimpleRequestInfoBinding;
import net.sf.j2s.ajax.SimplePipeRequest;
import net.sf.j2s.ajax.SimplePipeRunnable;
import net.sf.j2s.ajax.SimpleRPCRequest;
import net.sf.j2s.ajax.SimpleSerializable;
import net.sf.j2s.ajax.annotation.SimpleIn;

/**
 * Base class for Login2ServicePipe, Service2LoginPipe, Service2ServicePipe.
 * 
 * @author zhourenjian
 *
 */
public abstract class AbstractClusterPipe extends SimplePipeRunnable implements
		ISimpleRequestInfoBinding, Comparable<AbstractClusterPipe> {

	/**
	 * @See ClusterConfig.clusterAPISecret and ClusterConfig.clusterGatewaySecret
	 */
	@SimpleIn
	public String apiSecret;
	
	private long lastHeartbeat;

	private String ip;

	private long lastTime;
	
	private long delaying;
	
	private int retries;
	
	private static Set<AbstractClusterPipe> allMonitoredPipes4Server = new ConcurrentSkipListSet<AbstractClusterPipe>();
	
	private static boolean thread4ServerStarted = false;
	
	public static void startClusterPipeMonitor4Server() {
		if (thread4ServerStarted) {
			return;
		}
		synchronized (AbstractClusterPipe.class) {
			if (thread4ServerStarted) {
				return;
			}
			thread4ServerStarted = true;
		}
		Thread heartbeatThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(ClusterConfig.clusterServerHeartBeatInterval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					long now = System.currentTimeMillis();
					HeartBeat e = new HeartBeat();
					e.time = now;
					for (Iterator<AbstractClusterPipe> itr = allMonitoredPipes4Server.iterator(); itr.hasNext();) {
						AbstractClusterPipe p = (AbstractClusterPipe) itr.next();
						if (p != null && p.isPipeLive()/* && !p.hasPipeData()*/) {
							p.pipeThrough(e);
						}
					}
				}
			}
			
		}, "Cluster Pipe Heartbeat Generator");
		heartbeatThread.setDaemon(true);
		heartbeatThread.start();
	}

	private static Set<AbstractClusterPipe> allMonitoredPipes4Client = new ConcurrentSkipListSet<AbstractClusterPipe>();
	
	private static boolean thread4ClientStarted = false;
	
	public static void removeClusterPipeFromMonitor4Client(AbstractClusterPipe pipe) {
		allMonitoredPipes4Client.remove(pipe);
	}
	
	public static void removeClusterPipeFromMonitor4Server(AbstractClusterPipe pipe) {
		allMonitoredPipes4Server.remove(pipe);
	}
	
	public static void startClusterPipeMonitor4Client(AbstractClusterPipe pipe) {
		pipe.updateStatus(true);
		allMonitoredPipes4Client.add(pipe);
		if (thread4ClientStarted) {
			return;
		}
		synchronized (AbstractClusterPipe.class) {
			if (thread4ClientStarted) {
				return;
			}
			thread4ClientStarted = true;
		}
		Thread monitorThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(ClusterConfig.clusterPipeMonitorInterval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (Iterator<AbstractClusterPipe> itr = allMonitoredPipes4Client.iterator(); itr.hasNext();) {
						final AbstractClusterPipe p = (AbstractClusterPipe) itr.next();
						if (p != null && p.pipeKey != null && !p.isPipeLive()) {
							if (ClusterConfig.clusterLogging) {
								System.out.println("Cluster: Try to close pipe " + p.getClass().getSuperclass().getName() + " / " + p.getHttpURL());
							}
							p.setLastHeartbeat(System.currentTimeMillis());
							
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
							
							//p.pipeClosed();
						}
					}
				}
			}
			
		}, "Cluster Pipe Live Montior");
		monitorThread.setDaemon(true);
		monitorThread.start();
	}
	
	private static Object reconnecting4ClientMutex = new Object();
	
	private static Set<AbstractClusterPipe> allDelayedPipes4Client = new ConcurrentSkipListSet<AbstractClusterPipe>();
	
	private static boolean reconnecting4ClientStarted = false;

	public static void startClusterPipeReconnecting4Client() {
		if (reconnecting4ClientStarted) {
			return;
		}
		synchronized (AbstractClusterPipe.class) {
			if (reconnecting4ClientStarted) {
				return;
			}
			reconnecting4ClientStarted = true;
		}
		Thread reconnectThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					synchronized (reconnecting4ClientMutex) {
						try {
							if (allDelayedPipes4Client.size() > 0) {
								reconnecting4ClientMutex.wait(ClusterConfig.clusterReconnectIncrementalInterval);
							} else {
								reconnecting4ClientMutex.wait();
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					for (Iterator<AbstractClusterPipe> itr = allDelayedPipes4Client.iterator(); itr.hasNext();) {
						AbstractClusterPipe pipe = (AbstractClusterPipe) itr.next();
						long now = System.currentTimeMillis();
						if (Math.abs(now - pipe.lastTime) >= pipe.delaying) {
							if (!pipe.isPipeLive()) { // No other threads have reconnected this pipe
								long heartbeat = pipe.lastHeartbeat;
								pipe.pipeKey = null;
								pipe.updateStatus(true);
								pipe.lastHeartbeat = heartbeat;
								pipe.setSequence(1);
								SimplePipeRequest.pipe(pipe);
							}
							allDelayedPipes4Client.remove(pipe);
						}
					}
				} // end of while
			}
			
		}, "Cluster Pipe Reconnecting Monitor");
		reconnectThread.setDaemon(true);
		reconnectThread.start();
	}

	protected void delayReconnectPipe(AbstractClusterPipe pipe) {
		if (Math.abs(System.currentTimeMillis() - pipe.lastHeartbeat) > Math.max(ClusterConfig.clusterReconnectMaxTime, ClusterConfig.clusterPipeHeartBeatTimeoutInterval)
				&& pipe.retries > ClusterConfig.clusterReconnectMinimumRetries && !(pipe instanceof Login2ServicePipe)) {
			pipe.pipeRemoved();
			return;
		}
		if (pipe.isPipeLive()) {
			pipe.updateStatus(false); // Make sure pipe is not alive
		}
		
		startClusterPipeReconnecting4Client();
		synchronized (reconnecting4ClientMutex) {
			allDelayedPipes4Client.add(pipe);
			reconnecting4ClientMutex.notify();
		}
		
		lastTime = System.currentTimeMillis();
		delaying += ClusterConfig.clusterReconnectIncrementalInterval + delaying * ClusterConfig.clusterReconnectIncrementalRate;
		retries++;
		long maxDelay = ClusterConfig.clusterReconnectFixedInterval + ClusterConfig.clusterReconnectFixedPerRetryInterval * retries;
		if (maxDelay > ClusterConfig.clusterReconnectMaxInterval) {
			maxDelay = ClusterConfig.clusterReconnectMaxInterval;
		}
		if (delaying > maxDelay) {
			delaying = maxDelay;
		}
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: Reconnecting " + pipe.getClass().getSuperclass().getName() + " / " + pipe.getHttpURL() + " : " + delaying + " - " + retries);
		}
	}
	
	public long getLastHeartbeat() {
		return lastHeartbeat;
	}

	public void setLastHeartbeat(long lastHeartbeat) {
		this.lastHeartbeat = lastHeartbeat;
	}

	protected void pipeRemoved() {
		// pipe is totally removed and disconnected with future reconnecting
		this.pipeDestroy();
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: " + getClass().getSuperclass().getName() + " removed / " + getHttpURL() + " : " + delaying + " - " + retries);
		}
	}

	@Override
	public void setRemoteUserAgent(String userAgent) {
	}

	@Override
	public void setReferer(String referer) {
	}

	@Override
	public void setRequestURL(String url) {
	}

	@Override
	public void setRequestHost(String host) {
	}

	@Override
	public void setRemoteIP(String ip) {
		this.ip = ip;
	}

	public String getRemoteIP() {
		return ip;
	}

	@Override
	public void setLanguages(String[] language) {
	}
	
	@Override
	public boolean pipeSetup() {
		if (!ClusterServer.isClusterServer()) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: This server is not a cluster server!");
			}
			return false; // silently
		}
		if (!XYZMixxer.isAPISecretOK(apiSecret)) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: API secret " + apiSecret + " authorization failed!");
			}
			return false; // silently
		}
		allMonitoredPipes4Server.add(this);
		startClusterPipeMonitor4Server();
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: Server " + ip + " connected to " + this.getClass().getName());
		}
		return true;
	}

	@Override
	public void pipeCreated() {
		super.pipeCreated();
		delaying = 0;
		retries = 0;
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: " + this.getClass().getSuperclass().getName() + " connected / " + getHttpURL());
		}
	}

	@Override
	public boolean pipeDestroy() {
		allMonitoredPipes4Server.remove(this);
		return super.pipeDestroy();
	}
	
	@Override
	protected void pipeClearData() {
		// do nothing to keep existed pipe data
	}

	@Override
	public int compareTo(AbstractClusterPipe o) {
		return hashCode() - o.hashCode();
	}
	
}
