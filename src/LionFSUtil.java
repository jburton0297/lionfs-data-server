

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class LionFSUtil {

	public static final String TEMP_PATH = "temp/";
	
	public static void writeInt(Socket socket, int i) throws IOException {
		OutputStream os = socket.getOutputStream();
		byte[] b = ByteBuffer.allocate(4).putInt(i).array();
		os.write(b, 0, b.length);
		os.flush();
	}
	
	public static void writeString(Socket socket, String s) throws IOException {
		OutputStream os = socket.getOutputStream();
		byte[] sb = s.getBytes();
		byte[] b = new byte[32];
		for(int i = 0; i < b.length; i++) {
			if(i < sb.length) {
				b[i] = sb[i];
			}
		}
		os.write(b, 0, b.length);
		os.flush();
	}
	
	public static synchronized void writeFile(Socket socket, File f) throws IOException {
		OutputStream os = socket.getOutputStream();
		byte[] nameb = f.getName().getBytes();
		byte[] b = new byte[32];
		for(int i = 0; i < b.length; i++) {
			if(i < nameb.length) {
				b[i] = nameb[i];
			} 
		}
		os.write(b, 0, b.length);
		os.flush();
		byte[] size = ByteBuffer.allocate(4).putInt((int) f.length()).array();
		os.write(size);
		os.flush();
		byte[] data = Files.readAllBytes(f.toPath());
		os.write(data, 0, data.length);
		os.flush();
	}
	
	public static void writeBytes(Socket socket, byte[] bytes) throws IOException {
		OutputStream os = socket.getOutputStream();
		os.write(bytes, 0, bytes.length);
		os.flush();
	}
	
	public static int readInt(Socket socket) throws IOException {
		InputStream is = socket.getInputStream();
		byte[] b = new byte[Integer.BYTES];
		is.read(b, 0, b.length);
		int i = ByteBuffer.wrap(b).getInt();
		return i;
	}
	
	public static String readString(Socket socket) throws IOException {
		InputStream is = socket.getInputStream();
		byte[] b = new byte[32];
		is.read(b, 0, b.length);
		String s = new String(b);
		return s.trim();
	}
	
	public static byte[] readFileAsBytes(Socket socket) throws IOException {
		InputStream is = socket.getInputStream();
//		byte[] nameb = new byte[32];
//		is.read(nameb, 0, nameb.length);
//		String name = new String(nameb);
		byte[] sizeb = new byte[Integer.BYTES];
		is.read(sizeb, 0, sizeb.length);
		int size = ByteBuffer.wrap(sizeb).getInt();
		byte[] data = new byte[size];
		is.read(data, 0, data.length);
		return data;
	}
	
	public static byte[] readBytes(Socket socket, int size) throws IOException {
		InputStream is = socket.getInputStream();
		byte[] bytes = new byte[size];
		is.read(bytes, 0, bytes.length);
		return bytes;
	}
	
	public static synchronized void writeBytesToFile(byte[] data, File file) throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
		fos.write(data);
		fos.close();
	}
	
}
