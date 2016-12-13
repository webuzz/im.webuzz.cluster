package im.webuzz.cluster;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.sf.j2s.ajax.SimpleFilter;
import net.sf.j2s.ajax.SimpleSerializable;
import net.sf.j2s.ajax.annotation.SimpleComment;
import net.sf.j2s.annotation.J2SIgnore;

/**
 * Base class of all cluster events. All application related events should
 * inherit from ClusterEvent.
 * 
 * Events used in cluster server, e.g. ServerHeartBeat, ServerStatus and 
 * UserServer* events. have no relationship with ClusterEvent.
 * 
 * This class overrides some methods of super class SimpleSerializable in order
 * to serialize and deserialize all sub-classes objects without knowing their
 * inside details, e.g. class name, fields.
 * 
 * @author zhourenjian
 *
 */
public class ClusterEvent extends SimpleSerializable {
	
	@SimpleComment({
		"For server pipes, target user uid. Once set, it will never be changed.",
		"Client side should ignore this field."
	})
	public int receiver;
	
	@SimpleComment({
		"from user; this field will be used for optimization."
	})
	public int sender; 

	@J2SIgnore
	private boolean evtClassTransparent = ClusterConfig.clusterEventTransparent;
	
	@J2SIgnore
	private String evtContentBody;
	@J2SIgnore
	private byte[] evtContentBytes;
	@J2SIgnore
	private boolean evtSenderModified = false;
	
	@J2SIgnore
	private int evtSizeEnd = -1;
	@J2SIgnore
	private int evtSenderStart = -1;
	@J2SIgnore
	private int evtSenderEnd = -1;
	
	private static String[] mappings = new String[] {
		"sender", "s",
		"receiver", "r",
	};
	@J2SIgnore
	private static Map<String, String> nameMappings = mappingFromArray(mappings, false);
	@J2SIgnore
	private static Map<String, String> aliasMappings = mappingFromArray(mappings, true);

	@J2SIgnore
	@Override
	protected Map<String, String> fieldNameMapping() {
		return nameMappings;
	}

	@J2SIgnore
	@Override
	protected Map<String, String> fieldAliasMapping() {
		return aliasMappings;
	}

	@J2SIgnore
	public void setSender(int sender) {
		if (sender != this.sender) {
			this.sender = sender;
			evtSenderModified = true;
		}
	}

	@J2SIgnore
	@Override
	protected String serialize(SimpleFilter filter,
			List<SimpleSerializable> ssObjs, boolean supportsCompactBytes) {
		if (isEventSubclassTransparent() && (evtContentBody != null || evtContentBytes != null)) {
			if (evtContentBody == null) {
				evtContentBody = new String(evtContentBytes, ISO_8859_1);
			}
			if (!evtSenderModified) {
				return evtContentBody;
			}
			if (evtSizeEnd != -1 && evtSenderStart != -1 && evtSenderEnd != -1) {
				StringBuilder builder = new StringBuilder();
				builder.append(evtContentBody.substring(0, evtSenderStart));
				int senderSize = 0;
				if (sender != 0) {
					int ver = 201;
					try {
						ver = Integer.parseInt(evtContentBody.substring(3, 6));
						setSimpleVersion(ver); // keep old version
					} catch (NumberFormatException e) {
					}
					char baseChar = 'B';
					/*
					String name = ver >= 202 ? "s" : "sender";
					builder.append((char)(baseChar + name.length()));
					builder.append(name);
					builder.append('I');
					// */
					String value = String.valueOf(sender);
					builder.append(ver >= 202 ? "CsI" : "HsenderI");
					builder.append((char) (baseChar + value.length()));
					builder.append(value);
					senderSize = (ver >= 202 ? 4 : 9) + value.length();
				}
				builder.append(evtContentBody.substring(evtSenderEnd));
				String sizeStr = String.valueOf(evtContentBody.length() - evtSizeEnd - 1 - ((evtSenderEnd - evtSenderStart) - senderSize));
				builder.replace(evtSizeEnd - 8, evtSizeEnd, "00000000");
				builder.replace(evtSizeEnd - sizeStr.length(), evtSizeEnd, sizeStr); // update size!
				return builder.toString();
			}
		}
		return super.serialize(filter, ssObjs, supportsCompactBytes);
	}

	@J2SIgnore
	@Override
	protected DeserializeObject deserializeArrayItem(String str, int index,
			int end, List<SimpleSerializable> ssObjs) {
		if (!isEventSubclassTransparent()) {
			return super.deserializeArrayItem(str, index, end, ssObjs);
		}
		// For transparent mode, try to skip array elements
    	char baseChar = 'B';
		char c2 = str.charAt(index++);
		if (c2 == 'A' || c2 == 'Z' || c2 == 'Y') {
			c2 = str.charAt(index++);
			char c3 = str.charAt(index++);
			int l2 = c3 - baseChar;
			try {
				if (l2 < 0 && l2 != -2) {
					return new DeserializeObject(null, index);
				} else {
					if (l2 == -2) {
						char c4 = str.charAt(index++);
						int l3 = c4 - baseChar;
						if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
						try {
							l2 = Integer.parseInt(str.substring(index, index + l3));
						} catch (Exception e) {
							throw new RuntimeException("Invalid simple format.", e);
						}
						index += l3;
						if (l2 < 0) throw new RuntimeException("Invalid simple format.");
						if (l2 > 0x1000000) { // 16 * 1024 * 1024
							/*
							 * Some malicious string may try to allocate huge size of array!
							 * Limit the size of array here! 
							 */
							throw new RuntimeException("Array size reaches the limit of Java2Script Simple RPC!");
						}
					}
					if (c2 == '8') { // byte[]
						if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
						index += l2;
						return new DeserializeObject(null, index);
					}
					if (c2 == 'Z' || c2 == 'Y' || c2 == 'Q') {
						for (int i = 0; i < l2; i++) {
							DeserializeObject o = deserializeArrayItem(str, index, end, ssObjs);
							index = o.index;
						}
						return new DeserializeObject(null, index);
					} else if (c2 == 'M') {
						for (int i = 0; i < l2 / 2; i++) {
							DeserializeObject key = deserializeArrayItem(str, index, end, ssObjs);
							index = key.index;
							DeserializeObject value = deserializeArrayItem(str, index, end, ssObjs);
							index = value.index;
						}
						return new DeserializeObject(null, index);
					}
					for (int i = 0; i < l2; i++) {
						char c4 = str.charAt(index++);
						if (c2 != 'X' && c2 != 'O') {
							int l3 = c4 - baseChar;
							if (l3 > 0) {
								if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
								index += l3;
							} else if (l3 == 0) {
							}
						} else {
							char c5 = str.charAt(index++);
							int l3 = c5 - baseChar;
							if (l3 > 0) {
								if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
								index += l3;
							} else if (l3 == 0) {
							} else if (l3 == -2) {
								char c6 = str.charAt(index++);
								int l4 = c6 - baseChar;
								if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
								int l5 = -1;
								try {
									l5 = Integer.parseInt(str.substring(index, index + l4));
								} catch (Exception e) {
									throw new RuntimeException("Invalid simple format.", e);
								}
								index += l4;
								if (l5 < 0 || index + l5 > end) throw new RuntimeException("Invalid simple format.");								index += l5;
							} else {
								continue;
							}
						}
					}
					return new DeserializeObject(null, index);
				}
			} catch (Exception e) {
				System.out.println("Parsing: " + str);
				e.printStackTrace();
			}
		} else {
			char c3 = str.charAt(index++);
			int l2 = c3 - baseChar;
			if (l2 > 0) {
				if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
				index += l2;
			} else if (l2 == 0) {
			} else if (l2 == -2) {
				char c4 = str.charAt(index++);
				int l3 = c4 - baseChar;
				if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
				int l4 = -1;
				try {
					l4 = Integer.parseInt(str.substring(index, index + l3));
				} catch (NumberFormatException e) {
					throw new RuntimeException("Invalid simple format.", e);
				}
				index += l3;
				if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
				index += l4;
			}
			return new DeserializeObject(null, index);
		}
		return new DeserializeObject(null, index);
	}
	
	@J2SIgnore
	@Override
	protected boolean deserialize(String str, int start,
			List<SimpleSerializable> ssObjs) {
		if (!isEventSubclassTransparent()) {
			return super.deserialize(str, start, ssObjs);
		}
		// For transparent mode, try to keep content body
		char baseChar = 'B';
		if (str == null || start < 0) return false;
		int end = str.length();
		int length = end - start;
		if (length <= 7) return false;
		if (!("WLL".equals(str.substring(start, start + 3)))) {
			throw new RuntimeException("Invalid simple format.");
		}
		try {
			setSimpleVersion(Integer.parseInt(str.substring(start + 3, start + 6)));
		} catch (NumberFormatException e1) {
		}
		int index = str.indexOf('#', start);
		if (index == -1) throw new RuntimeException("Invalid simple format.");
		index++;
		if (index >= end) return false; // may be empty string!
		
		int size = 0;
		char nextChar = str.charAt(index);
		if (nextChar >= '0' && nextChar <= '9') {
			// have size!
			int last = index;
			index = str.indexOf('$', last);
			if (index == -1) throw new RuntimeException("Invalid simple format.");
			evtSizeEnd = index - start;
			for (int i = last + 1; i < index; i++) {
				char c = str.charAt(i);
				if (c != '0') {
					try {
						size = Integer.parseInt(str.substring(i, index));
					} catch (NumberFormatException e) {
						throw new RuntimeException("Invalid simple format.", e);
					}
					break;
				}
			}
			// all fields are in their default values or no fields
			if (size == 0) return true;
			index++;
			// may be empty string or not enough string!
			if (index + size > end) return false;
		}
		if (start == 0 && index + size == end) {
			evtContentBody = str;
		} else {
			evtContentBody = str.substring(start, index + size);
		}
		int objectEnd = index + size;
		boolean senderParsed = false;
		boolean receiverParsed = false;
		while (index < end && index < objectEnd) {
			int fieldStart = index;
			char c1 = str.charAt(index++);
			int l1 = c1 - baseChar;
			if (l1 < 0 || index + l1 > end) throw new RuntimeException("Invalid simple format.");
			String fieldName = str.substring(index, index + l1);
			index += l1;
			char c2 = str.charAt(index++);
			if (c2 == 'A' || c2 == 'Z' || c2 == 'Y') {
				// Skip array
				c2 = str.charAt(index++);
				char c3 = str.charAt(index++);
				int l2 = c3 - baseChar;
				try {
					if (l2 < 0 && l2 != -2) {
					} else {
						if (l2 == -2) {
							char c4 = str.charAt(index++);
							int l3 = c4 - baseChar;
							if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
							try {
								l2 = Integer.parseInt(str.substring(index, index + l3));
							} catch (Exception e) {
								e.printStackTrace();
								throw new RuntimeException("Invalid simple format.", e);
							}
							index += l3;
							if (l2 < 0) throw new RuntimeException("Invalid simple format.");
							if (l2 > 0x1000000) { // 16 * 1024 * 1024
								/*
								 * Some malicious string may try to allocate huge size of array!
								 * Limit the size of array here! 
								 */
								throw new RuntimeException("Array size reaches the limit of Java2Script Simple RPC!");
							}
						}
						if (c2 == '8') { // byte[]
							if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
							index += l2;
							continue;
						}
						if (c2 == 'W') {
							continue;
						}
						if (c2 == 'Z' || c2 == 'Y' || c2 == 'Q') {
							for (int i = 0; i < l2; i++) {
								DeserializeObject o = deserializeArrayItem(str, index, end, ssObjs);
								index = o.index;
							}
							continue;
						} else if (c2 == 'M') {
							for (int i = 0; i < l2 / 2; i++) {
								DeserializeObject key = deserializeArrayItem(str, index, end, ssObjs);
								index = key.index;
								DeserializeObject value = deserializeArrayItem(str, index, end, ssObjs);
								index = value.index;
							}
							continue;
						}
						for (int i = 0; i < l2; i++) {
							char c4 = str.charAt(index++);
							if (c2 != 'X' && c2 != 'O') {
								int l3 = c4 - baseChar;
								if (l3 > 0) {
									if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
									index += l3;
								} else if (l3 == 0) {
								}
							} else {
								char c5 = str.charAt(index++);
								int l3 = c5 - baseChar;
								if (l3 > 0) {
									if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
									index += l3;
								} else if (l3 == 0) {
								} else if (l3 == -2) {
									char c6 = str.charAt(index++);
									int l4 = c6 - baseChar;
									if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
									int l5 = -1;
									try {
										l5 = Integer.parseInt(str.substring(index, index + l4));
									} catch (Exception e) {
										throw new RuntimeException("Invalid simple format.", e);
									}
									index += l4;
									if (l5 < 0 || index + l5 > end) throw new RuntimeException("Invalid simple format.");
									index += l5;
								} else {
									continue;
								}
							}
						}
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} else {
				char c3 = str.charAt(index++);
				int l2 = c3 - baseChar;
				if (l2 > 0) {
					if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
					index += l2;
					if ("r".equals(fieldName) || "receiver".equals(fieldName)) {
						String s = str.substring(index - l2, index);
						try {
							this.receiver = Integer.parseInt(s);
						} catch (NumberFormatException e) {
							e.printStackTrace();
						}
						receiverParsed = true;
						if (senderParsed) {
							return true; // all ClusterEvent fields parsed.
						}
					} else if ("s".equals(fieldName) || "sender".equals(fieldName)) {
						String s = str.substring(index - l2, index);
						try {
							this.sender = Integer.parseInt(s);
						} catch (NumberFormatException e) {
							e.printStackTrace();
						}
						evtSenderStart = fieldStart - start;
						evtSenderEnd = index - start;
						senderParsed = true;
						if (receiverParsed) {
							return true; // all ClusterEvent fields parsed.
						}
					}
				} else if (l2 == 0) {
				} else if (l2 == -2) {
					char c4 = str.charAt(index++);
					int l3 = c4 - baseChar;
					if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
					int l4 = -1;
					try {
						l4 = Integer.parseInt(str.substring(index, index + l3));
					} catch (NumberFormatException e) {
						throw new RuntimeException("Invalid simple format.", e);
					}
					index += l3;
					if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
					index += l4;
				}
			}
		}
		return true;
	}

	@J2SIgnore
	@Override
	protected byte[] serializeBytes(SimpleFilter filter,
			List<SimpleSerializable> ssObjs, boolean supportsCompactBytes)
			throws IOException {
		if (isEventSubclassTransparent() && (evtContentBytes != null || evtContentBody != null)) {
			if (evtContentBytes == null) {
				evtContentBytes = evtContentBody.getBytes(ISO_8859_1);
			}
			if (!evtSenderModified) {
				return evtContentBytes;
			}
			if (evtSizeEnd != -1 && evtSenderStart != -1 && evtSenderEnd != -1) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
				DataOutputStream dos = new DataOutputStream(baos);
				dos.write(evtContentBytes, 0, evtSenderStart);
				int senderSize = 0;
				if (sender != 0) {
					int ver = 100 * evtContentBytes[3] + 10 * evtContentBytes[4] + evtContentBytes[5] - '0' * 111;
					char baseChar = 'B';
					/*
					String name = ver >= 202 ? "s" : "sender";
					dos.writeByte((byte) (baseChar + name.length()));
					dos.writeBytes(name);
					dos.writeByte((byte) 'I');
					// */
					String value = String.valueOf(sender);
					dos.writeBytes(ver >= 202 ? "CsI" : "HsenderI");
					dos.writeByte((byte) (baseChar + value.length()));
					dos.writeBytes(value);
					senderSize = (ver >= 202 ? 4 : 9) + value.length();
				}
				dos.write(evtContentBytes, evtSenderEnd, evtContentBytes.length - evtSenderEnd);
				int size = dos.size();
				if (size > 0x1000000) { // 16 * 1024 * 1024
					throw new RuntimeException("Data size reaches the limit of Java2Script Simple RPC!");
				}
				// update size!
				String sizeStr = String.valueOf(evtContentBytes.length - evtSizeEnd - 1 - ((evtSenderEnd - evtSenderStart) - senderSize));
				int sizeLength = sizeStr.length();
				byte[] bytes = baos.toByteArray();
				for (int i = 0; i < 8 - sizeLength; i++) {
					bytes[evtSizeEnd - 8 + i] = (byte) '0';
				}
				for (int i = 0; i < sizeLength; i++) {
					bytes[evtSizeEnd - sizeLength + i] = (byte) sizeStr.charAt(i);
				}
				return bytes;
			}
		}
		return super.serializeBytes(filter, ssObjs, supportsCompactBytes);
	}
	
	@J2SIgnore
	@Override
    protected DeserializeObject deserializeBytesArrayItem(byte[] bytes, int index, int end, List<SimpleSerializable> ssObjs) {
    	if (!isEventSubclassTransparent()) {
    		return super.deserializeBytesArrayItem(bytes, index, end, ssObjs);
    	}
    	char baseChar = 'B';
		char c2 = (char) bytes[index++];
		if (c2 == 'A' || c2 == 'Z' || c2 == 'Y') {
			c2 = (char) bytes[index++];
			char c3 = (char) bytes[index++];
			int l2 = c3 - baseChar;
			try {
				if (l2 < 0 && l2 != -2) {
					return new DeserializeObject(null, index);
				} else {
					if (l2 == -2) {
						char c4 = (char) bytes[index++];
						int l3 = c4 - baseChar;
						if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
						try {
							l2 = Integer.parseInt(new String(bytes, index, l3));
						} catch (Exception e) {
							throw new RuntimeException("Invalid simple format.", e);
						}
						index += l3;
						if (l2 < 0) throw new RuntimeException("Invalid simple format.");
						if (l2 > 0x1000000) { // 16 * 1024 * 1024
							/*
							 * Some malicious string may try to allocate huge size of array!
							 * Limit the size of array here! 
							 */
							throw new RuntimeException("Array size reaches the limit of Java2Script Simple RPC!");
						}
					}
					if (c2 == '8') { // byte[]
						if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
						index += l2;
						return new DeserializeObject(null, index);
					}
					if (c2 == 'Z' || c2 == 'Y' || c2 == 'Q') {
						for (int i = 0; i < l2; i++) {
							DeserializeObject o = deserializeBytesArrayItem(bytes, index, end, ssObjs);
							index = o.index;
						}
						return new DeserializeObject(null, index);
					} else if (c2 == 'M') {
						for (int i = 0; i < l2 / 2; i++) {
							DeserializeObject key = deserializeBytesArrayItem(bytes, index, end, ssObjs);
							index = key.index;
							DeserializeObject value = deserializeBytesArrayItem(bytes, index, end, ssObjs);
							index = value.index;
						}
						return new DeserializeObject(null, index);
					}
					for (int i = 0; i < l2; i++) {
						char c4 = (char) bytes[index++];
						if (c2 != 'X' && c2 != 'O') {
							int l3 = c4 - baseChar;
							if (l3 > 0) {
								if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
								index += l3;
							} else if (l3 == 0) {
							}
						} else {
							char c5 = (char) bytes[index++];
							int l3 = c5 - baseChar;
							if (l3 > 0) {
								if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
								index += l3;
							} else if (l3 == 0) {
							} else if (l3 == -2) {
								char c6 = (char) bytes[index++];
								int l4 = c6 - baseChar;
								if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
								int l5 = -1;
								try {
									l5 = Integer.parseInt(new String(bytes, index, l4));
								} catch (Exception e) {
									throw new RuntimeException("Invalid simple format.", e);
								}
								index += l4;
								if (l5 < 0 || index + l5 > end) throw new RuntimeException("Invalid simple format.");
								index += l5;
							} else {
								continue;
							}
						}
					}
					return new DeserializeObject(null, index);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			char c3 = (char) bytes[index++];
			int l2 = c3 - baseChar;
			if (l2 > 0) {
				if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
				index += l2;
			} else if (l2 == 0) {
			} else if (l2 == -2) {
				char c4 = (char) bytes[index++];
				int l3 = c4 - baseChar;
				if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
				int l4 = -1;
				try {
					l4 = Integer.parseInt(new String(bytes, index, l3));
				} catch (NumberFormatException e) {
					throw new RuntimeException("Invalid simple format.", e);
				}
				index += l3;
				if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
				index += l4;
			}
			return new DeserializeObject(null, index);
		}
    }

	@J2SIgnore
	@Override
	protected boolean deserializeBytes(byte[] bytes, int start,
			List<SimpleSerializable> ssObjs) {
		if (!isEventSubclassTransparent()) {
			return super.deserializeBytes(bytes, start, ssObjs);
		}
		char baseChar = 'B';
		if (bytes == null || start < 0) return false;
		int end = bytes.length;
		int length = end - start;
		if (length <= 7) return false;
		if ('W' != bytes[start] || 'L' != bytes[start + 1] || 'L' != bytes[start + 2]) {
			throw new RuntimeException("Invalid simple format.");
		}
		setSimpleVersion(100 * bytes[start + 3] + 10 * bytes[start + 4] + bytes[start + 5] - '0' * 111);
		int index = bytesIndexOf(bytes, (byte) '#', start);
		if (index == -1) throw new RuntimeException("Invalid simple format.");
		index++;
		if (index >= end) return false; // may be empty string!
		
		int size = 0;
		char nextChar = (char) bytes[index];
		if (nextChar >= '0' && nextChar <= '9') {
			// have size!
			int last = index;
			index = bytesIndexOf(bytes, (byte) '$', last);
			if (index == -1) throw new RuntimeException("Invalid simple format.");
			evtSizeEnd = index - start;
			for (int i = last + 1; i < index; i++) {
				if (bytes[i] != '0') {
					for (; i < index; i++) {
						size = ((size << 3) + (size << 1)) + (bytes[i] - '0'); // size * 10
					}
//					try {
//						size = Integer.parseInt(new String(bytes, i, index - i));
//					} catch (NumberFormatException e) {
//						throw new RuntimeException("Invalid simple format.", e);
//					}
					break;
				}
			}
			// all fields are in their default values or no fields
			if (size == 0) return true;
			index++;
			// may be empty string or not enough string!
			if (index + size > end) return false;
		}
		if (start == 0 && index + size == end) {
			evtContentBytes = bytes;
		} else {
			int byteSize = index + size - start;
			byte[] evtBytes = new byte[byteSize];
			System.arraycopy(bytes, start, evtBytes, 0, byteSize);
			evtContentBytes = evtBytes;
		}

		int objectEnd = index + size;
		boolean senderParsed = false;
		boolean receiverParsed = false;
		while (index < end && index < objectEnd) {
			int fieldStart = index;
			char c1 = (char) bytes[index++];
			int l1 = c1 - baseChar;
			if (l1 < 0 || index + l1 > end) throw new RuntimeException("Invalid simple format.");
			String fieldName = new String(bytes, index, l1);
			index += l1;
			char c2 = (char) bytes[index++];
			if (c2 == 'A' || c2 == 'Z' || c2 == 'Y') {
				c2 = (char) bytes[index++];
				char c3 = (char) bytes[index++];
				int l2 = c3 - baseChar;
				try {
					if (l2 < 0 && l2 != -2) {
						continue;
					} else {
						if (l2 == -2) {
							char c4 = (char) bytes[index++];
							int l3 = c4 - baseChar;
							if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
							try {
								l2 = Integer.parseInt(new String(bytes, index, l3));
							} catch (Exception e) {
								throw new RuntimeException("Invalid simple format.", e);
							}
							index += l3;
							if (l2 < 0) throw new RuntimeException("Invalid simple format.");
							if (l2 > 0x1000000) { // 16 * 1024 * 1024
								/*
								 * Some malicious string may try to allocate huge size of array!
								 * Limit the size of array here! 
								 */
								throw new RuntimeException("Array size reaches the limit of Java2Script Simple RPC!");
							}
						}
						if (c2 == '8') { // byte[]
							if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
							index += l2;
							continue;
						}
						if (c2 == 'W') {
							continue;
						}
						if (c2 == 'Z' || c2 == 'Y' || c2 == 'Q') {
							for (int i = 0; i < l2; i++) {
								DeserializeObject o = deserializeBytesArrayItem(bytes, index, end, ssObjs);
								index = o.index;
							}
							continue;
						} else if (c2 == 'M') {
							for (int i = 0; i < l2 / 2; i++) {
								DeserializeObject key = deserializeBytesArrayItem(bytes, index, end, ssObjs);
								index = key.index;
								DeserializeObject value = deserializeBytesArrayItem(bytes, index, end, ssObjs);
								index = value.index;
							}
							continue;
						}
						for (int i = 0; i < l2; i++) {
							char c4 = (char) bytes[index++];
							if (c2 != 'X' && c2 != 'O') {
								int l3 = c4 - baseChar;
								if (l3 > 0) {
									if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
									index += l3;
								} else if (l3 == 0) {
								}
							} else {
								char c5 = (char) bytes[index++];
								int l3 = c5 - baseChar;
								if (l3 > 0) {
									if (index + l3 > end) throw new RuntimeException("Invalid simple format.");
									index += l3;
								} else if (l3 == 0) {
								} else if (l3 == -2) {
									char c6 = (char) bytes[index++];
									int l4 = c6 - baseChar;
									if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
									int l5 = -1;
									try {
										l5 = Integer.parseInt(new String(bytes, index, l4));
									} catch (Exception e) {
										throw new RuntimeException("Invalid simple format.", e);
									}
									index += l4;
									if (l5 < 0 || index + l5 > end) throw new RuntimeException("Invalid simple format.");
									index += l5;
								} else {
									continue;
								}
							}
						}
						continue;
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} else {
				char c3 = (char) bytes[index++];
				int l2 = c3 - baseChar;
				if (l2 > 0) {
					if (index + l2 > end) throw new RuntimeException("Invalid simple format.");
					index += l2;
					if (c2 != 'u') {
						String s = new String(bytes, index - l2, l2);
						if ("r".equals(fieldName) || "receiver".equals(fieldName)) {
							try {
								this.receiver = Integer.parseInt(s);
							} catch (NumberFormatException e) {
								e.printStackTrace();
							}
							receiverParsed = true;
							if (senderParsed) {
								return true; // all ClusterEvent fields parsed.
							}
						} else if ("s".equals(fieldName) || "sender".equals(fieldName)) {
							try {
								this.sender = Integer.parseInt(s);
							} catch (NumberFormatException e) {
								e.printStackTrace();
							}
							evtSenderStart = fieldStart - start;
							evtSenderEnd = index - start;
							senderParsed = true;
							if (receiverParsed) {
								return true; // all ClusterEvent fields parsed.
							}
						}
					}
				} else if (l2 == -2) {
					char c4 = (char) bytes[index++];
					int l3 = c4 - baseChar;
					if (l3 < 0 || index + l3 > end) throw new RuntimeException("Invalid simple format.");
					int l4 = -1;
					try {
						l4 = Integer.parseInt(new String(bytes, index, l3));
					} catch (NumberFormatException e) {
						throw new RuntimeException("Invalid simple format.", e);
					}
					index += l3;
					if (l4 < 0 || index + l4 > end) throw new RuntimeException("Invalid simple format.");
					index += l4;
				}
			}
		}
		return true;
	}

	/**
	 * Return whether serialization needs to know details of subclass.
	 * It is configurable by ClusterConfig#clusterEventTransparent.
	 * @return
	 */
	@J2SIgnore
	protected boolean isEventSubclassTransparent() {
		return evtClassTransparent;
	}
	
	@J2SIgnore
	public void setCallback(Runnable cb) {
	}

	@J2SIgnore
	public Runnable getCallback() {
		return null;
	}
	
}
