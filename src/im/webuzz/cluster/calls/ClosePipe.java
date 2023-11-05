package im.webuzz.cluster.calls;

import java.util.Iterator;
import java.util.Map;

import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.pipes.AbstractClusterPipe;
import im.webuzz.cluster.pipes.Login2LoginPipe;
import im.webuzz.cluster.pipes.Login2ServicePipe;
import im.webuzz.cluster.pipes.Service2LoginPipe;
import im.webuzz.cluster.pipes.Service2ServicePipe;
import im.webuzz.cluster.xyz.XYZMixxer;
import net.sf.j2s.ajax.SimplePipeHelper;
import net.sf.j2s.ajax.SimplePipeRunnable;
import net.sf.j2s.ajax.SimpleRPCRunnable;
import net.sf.j2s.ajax.annotation.SimpleIn;

public class ClosePipe extends SimpleRPCRunnable {

	private static String[] mappings = new String[] {
			"apiSecret", "a",
			"pipeKey", "p",
	};
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	@SimpleIn
	public String apiSecret;
	
	@SimpleIn
	public String pipeKey;

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
	public void ajaxRun() {
		if (!ClusterServer.isClusterServer()) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: This server is not a cluster server!");
			}
			return; // silently
		}
		if (!XYZMixxer.isAPISecretOK(apiSecret)) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: API secret " + apiSecret + " authorization failed!");
			}
			return; // silently
		}
		closePipe(pipeKey);
	}
	
	public static void closePipe(String pipeKey) {
		SimplePipeRunnable p = SimplePipeHelper.getPipe(pipeKey, true);
		if (p != null) {
			if (p instanceof Login2ServicePipe) {
				p.pipeClosed();
				Login2ServicePipe pipe = (Login2ServicePipe) p;
				AbstractClusterPipe.removeClusterPipeFromMonitor4Server(pipe);
				for (Iterator<Login2ServicePipe> itr = Login2ServicePipe.allLoginPipes.values().iterator(); itr.hasNext();) {
					Login2ServicePipe loginPipe = itr.next();
					if (p == loginPipe) {
						itr.remove();
						break;
					}
				}
			} else if (p instanceof Service2LoginPipe) {
				p.pipeClosed();
				Service2LoginPipe pipe = (Service2LoginPipe) p;
				AbstractClusterPipe.removeClusterPipeFromMonitor4Server(pipe);
				synchronized (Service2LoginPipe.pipeMutex) {
					if (Service2LoginPipe.servicePipe == pipe) {
						Service2LoginPipe.servicePipe = null;
					}
				}
			} else if (p instanceof Service2ServicePipe) {
				p.pipeClosed();
				Service2ServicePipe pipe = (Service2ServicePipe) p;
				AbstractClusterPipe.removeClusterPipeFromMonitor4Server(pipe);
				for (Iterator<Service2ServicePipe> itr = Service2ServicePipe.allServicePipes.values().iterator();
						itr.hasNext();) {
					Service2ServicePipe servicePipe = (Service2ServicePipe) itr.next();
					if (p == servicePipe) {
						itr.remove();
						break;
					}
				}
			} else if (p instanceof Login2LoginPipe) {
				p.pipeClosed();
				Login2LoginPipe pipe = (Login2LoginPipe) p;
				AbstractClusterPipe.removeClusterPipeFromMonitor4Server(pipe);
				for (Iterator<Login2LoginPipe> itr = Login2LoginPipe.allSyncPipes.values().iterator(); itr.hasNext();) {
					Login2LoginPipe syncPipe = itr.next();
					if (p == syncPipe) {
						itr.remove();
						break;
					}
				}
			}
		} else {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: Null pipe for " + pipeKey);
			}
		}
	}

}
