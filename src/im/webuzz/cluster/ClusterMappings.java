package im.webuzz.cluster;

import im.webuzz.cluster.calls.ClosePipe;
import im.webuzz.cluster.calls.SendEvent;
import im.webuzz.cluster.calls.SynchronizeUsers;
import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.events.ServerLost;
import im.webuzz.cluster.events.ServerStatus;
import im.webuzz.cluster.events.ServerStopping;
import im.webuzz.cluster.events.UserDuplicated;
import im.webuzz.cluster.events.UserQuery;
import im.webuzz.cluster.events.UserResult;
import im.webuzz.cluster.events.UserStatus;
import im.webuzz.cluster.pipes.Login2LoginPipe;
import im.webuzz.cluster.pipes.Login2ServicePipe;
import im.webuzz.cluster.pipes.Service2LoginPipe;
import im.webuzz.cluster.pipes.Service2ServicePipe;
import net.sf.j2s.ajax.SimplePipeSequence;
import net.sf.j2s.ajax.SimpleSerializable;

/**
 * Make protocols more compacted.
 * 
 * @author zhourenjian
 *
 */
public class ClusterMappings {

	public static void initializeMappings() {
		// j2s : Don't change the following line
		SimpleSerializable.registerClassShortenName(SimplePipeSequence.class.getName(), "SPS");
		
		// cluster event
		SimpleSerializable.registerClassShortenName(HeartBeat.class.getName(), "cHB");
		SimpleSerializable.registerClassShortenName(ServerStatus.class.getName(), "cST");
		SimpleSerializable.registerClassShortenName(ServerLost.class.getName(), "cSO");
		SimpleSerializable.registerClassShortenName(ServerStopping.class.getName(), "cSP");
		SimpleSerializable.registerClassShortenName(UserDuplicated.class.getName(), "cUD");
		SimpleSerializable.registerClassShortenName(UserQuery.class.getName(), "cUQ");
		SimpleSerializable.registerClassShortenName(UserResult.class.getName(), "cUR");
		SimpleSerializable.registerClassShortenName(UserStatus.class.getName(), "cUS");
		// cluster pipe
		SimpleSerializable.registerClassShortenName(Login2LoginPipe.class.getName(), "cLL");
		SimpleSerializable.registerClassShortenName(Login2ServicePipe.class.getName(), "cLS");
		SimpleSerializable.registerClassShortenName(Service2LoginPipe.class.getName(), "cSL");
		SimpleSerializable.registerClassShortenName(Service2ServicePipe.class.getName(), "cSS");
		// cluster call
		SimpleSerializable.registerClassShortenName(ClusterSendEvent.class.getName(), "cCS");
		SimpleSerializable.registerClassShortenName(ClosePipe.class.getName(), "cCP");
		SimpleSerializable.registerClassShortenName(SendEvent.class.getName(), "cSE");
		SimpleSerializable.registerClassShortenName(SynchronizeUsers.class.getName(), "cSU");
	}
	
}
