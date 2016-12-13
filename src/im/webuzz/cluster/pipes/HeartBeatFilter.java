package im.webuzz.cluster.pipes;

import im.webuzz.cluster.events.HeartBeat;
import im.webuzz.cluster.events.ServerStopping;
import net.sf.j2s.ajax.SimpleFilter;
import net.sf.j2s.ajax.SimplePipeSequence;

/*
 * This filter will be used while cloning pipe data.
 * Pipe data is cloned to avoid being lost in pipe reconnecting.
 */
class HeartBeatFilter implements SimpleFilter {

	public static HeartBeatFilter singleton = new HeartBeatFilter();
	
	private static String shbClassName = HeartBeat.class.getName();
	
	private static String sspClassName = ServerStopping.class.getName();
	
	private static String spsClassName = SimplePipeSequence.class.getName();
	
	/*
	 * Return true for those events which need to be cloned.
	 * 
	 * (non-Javadoc)
	 * @see net.sf.j2s.ajax.SimpleFilter#accept(java.lang.String)
	 */
	@Override
	public boolean accept(String clazzName) {
		if (shbClassName.equals(clazzName)
				|| spsClassName.equals(clazzName)
				|| sspClassName.equals(clazzName)) {
			return false;
		}
		return true;
	}

	@Override
	public boolean ignoreDefaultFields() {
		return false;
	}

}
