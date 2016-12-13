package im.webuzz.cluster.calls;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.xyz.XYZMixxer;
import net.sf.j2s.ajax.ISimpleRequestInfoBinding;
import net.sf.j2s.ajax.SimpleRPCRequest;
import net.sf.j2s.ajax.SimpleRPCRunnable;
import net.sf.j2s.ajax.annotation.SimpleIn;
import net.sf.j2s.ajax.annotation.SimpleOut;

/**
 * The base class of all Cluster RPC. If the cluster RPC fails, it will use
 * cluster piping to fulfill the task.
 * 
 * @author zhourenjian
 *
 */
public abstract class AbstractClusterRPC extends SimpleRPCRunnable implements ISimpleRequestInfoBinding {

	public static final int OK = 1;

	public static final int ERROR = -1;

	@SimpleIn
	public String apiSecret;
	
	@SimpleIn
	public int serverPort; // source server
	
	@SimpleIn
	public String serverIP; // source server's specified IP
	
	@SimpleOut
	public int returnCode; // 1: OK, -1: Error
	
	private String ip;

	private int retries = 1;
	
	@Override
	public void ajaxRun() {
		returnCode = 0;
		if (!ClusterServer.isClusterServer()) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: This server is not a cluster server!");
			}
			returnCode = ERROR;
			return; // silently
		}
		if (!XYZMixxer.isAPISecretOK(apiSecret)) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: API secret " + apiSecret + " authorization failed!");
			}
			returnCode = ERROR;
			return; // silently
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
	public void setLanguages(String[] language) {
	}

	@Override
	public void setRemoteIP(String ip) {
		this.ip = ip;
	}

	public String getRemoteDomain() {
		if (serverIP != null && serverIP.length() > 0) {
			return serverIP;
		}
		return ip;
	}

	public int getRemotePort() {
		return serverPort;
	}
	
	public void setRetries(int count) {
		retries = count;
	}
	
	protected void onFallback() {
		// To be overrided
	}
	
	public void retryOrFallback() {
		if (retries <= 0) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: Fallback " + getHttpURL() + " " + this.serverIP + ":" + this.serverPort + " - " + this.getClass().getName() + " : " + this.getClass().getSuperclass().getName());
			}
			onFallback();
			return;
		}
		if (ClusterConfig.clusterLogging) {
			System.out.println("Cluster: Retry " + getHttpURL() + " " + this.serverIP + ":" + this.serverPort + " - " + this.getClass().getName() + " : " + this.getClass().getSuperclass().getName());
		}
		retries--;
		SimpleRPCRequest.request(this);
	}
	
}
