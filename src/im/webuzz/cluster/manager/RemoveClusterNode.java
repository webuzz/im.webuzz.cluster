package im.webuzz.cluster.manager;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.pipes.AbstractClusterPipe;
import im.webuzz.cluster.pipes.Service2LoginPipe;
import im.webuzz.cluster.pipes.Service2ServicePipe;
import im.webuzz.cluster.xyz.XYZMixxer;
import net.sf.j2s.ajax.SimpleRPCRunnable;

/*
 * We should have fixed bugs which leave disconnected servers in those
 * maps or list.
 */
@Deprecated
public class RemoveClusterNode extends SimpleRPCRunnable {

	public String apiSecret;
	
	public int serverPort; // [IN] server source
	
	public String serverIP; // [IN] server source' specified IP
	
	public int returnCode; // 1: OK, -1: Error
	
	@Override
	public void ajaxRun() {
		returnCode = 0;
		if (!ClusterServer.isClusterServer()) {
			returnCode = -1;
			return; // silently
		}
		if (!XYZMixxer.isAPISecretOK(apiSecret)) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: API secret " + apiSecret + " authorization failed!");
			}
			returnCode = -1;
			return; // silently
		}
		
		removeServer(serverPort == 80 ? serverIP : (serverIP + ":" + serverPort));
		returnCode = 1;
	}

	private void removeServer(String serverHost) {
		try {
			Field fPipes = XYZMixxer.class.getDeclaredField("allService2LoginPipes");
			fPipes.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<String, Service2LoginPipe> allService2LoginPipes = (Map<String, Service2LoginPipe>) fPipes.get(XYZMixxer.class);
			if (allService2LoginPipes != null) {
				AbstractClusterPipe pipe = allService2LoginPipes.remove(serverHost);
				if (pipe != null) {
					removePipe(pipe);
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		
		try {
			Field fPipes = XYZMixxer.class.getDeclaredField("allService2ServicePipes");
			fPipes.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<String, Service2ServicePipe> allService2ServicePipes = (Map<String, Service2ServicePipe>) fPipes.get(XYZMixxer.class);
			if (allService2ServicePipes != null) {
				AbstractClusterPipe pipe = allService2ServicePipes.remove(serverHost);
				if (pipe != null) {
					removePipe(pipe);
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	private void removePipe(AbstractClusterPipe pipe) {
		try {
			Field fPipes = AbstractClusterPipe.class.getDeclaredField("allMonitoredPipes4Server");
			fPipes.setAccessible(true);
			@SuppressWarnings("unchecked")
			Set<AbstractClusterPipe> allMonitoredPipes4Server = (Set<AbstractClusterPipe>) fPipes.get(AbstractClusterPipe.class);
			if (allMonitoredPipes4Server != null) {
				allMonitoredPipes4Server.remove(pipe);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}

		try {
			Field fPipes = AbstractClusterPipe.class.getDeclaredField("allMonitoredPipes4Client");
			fPipes.setAccessible(true);
			@SuppressWarnings("unchecked")
			Set<AbstractClusterPipe> allMonitoredPipes4Client = (Set<AbstractClusterPipe>) fPipes.get(AbstractClusterPipe.class);
			if (allMonitoredPipes4Client != null) {
				allMonitoredPipes4Client.remove(pipe);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}
