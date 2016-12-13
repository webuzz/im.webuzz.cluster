package im.webuzz.cluster.manager;

import net.sf.j2s.ajax.SimpleRPCRunnable;
import im.webuzz.cluster.ClusterConfig;
import im.webuzz.cluster.ClusterServer;
import im.webuzz.cluster.events.IStatus;
import im.webuzz.cluster.events.UserQuery;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.pipes.Service2LoginPipe;
import im.webuzz.cluster.xyz.XYZMixxer;

public class QueryUserClusterStatus extends SimpleRPCRunnable {

	public int uid;
	
	public String apiSecret;
	
	public int returnCode;
	
	public int status;
	
	public String domain;
	
	public int port;
	
	@Override
	public void ajaxRun() {
		returnCode = 0;
		if (!ClusterServer.isClusterServer()) {
			returnCode = -2;
			return; // silently
		}
		if (!XYZMixxer.isAPISecretOK(apiSecret)) {
			if (ClusterConfig.clusterLogging) {
				System.out.println("Cluster: API secret " + apiSecret + " authorization failed!");
			}
			returnCode = -2;
			return; // silently
		}
		int slot = uid & XYZMixxer.SLOTS_MASKS; // uid % XYZMixxer.SLOTS;
		if (ClusterConfig.clusterPrimaryServer) {
			// Primary login server knows which user on which server directly.
			// Select correct pipe and pipe out data event.
			returnCode = 1;
			synchronized (XYZMixxer.statusMutex[slot]) {
				UserResult r = XYZMixxer.allStatuses[slot].get(uid);
				if (r == null || r.status != IStatus.CONNECTED) {
					returnCode = 0;
				}
				if (r != null) {
					status = r.status;
					domain = r.domain;
					port = r.port;
				}
			}
			return;
		} else {
			// For normal service, try to check cached users. If there are
			// not-expired-yet cached user, try to pipe data through service-
			// service pipe for this cached user.
			synchronized (XYZMixxer.userMutex[slot]) {
				UserResult r = XYZMixxer.cachedUsers[slot].get(uid);
				if (r != null && (r.lastUpdated == -1
						|| Math.abs(System.currentTimeMillis() - r.lastUpdated) < ClusterConfig.clusterUserCachingTime)) {
					if (r.status != IStatus.CONNECTED) {
						returnCode = 0;
					}
					returnCode = 1;
					
					status = r.status;
					domain = r.domain;
					port = r.port;
					return;
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
			returnCode = -1;
		}
	}
	
}
