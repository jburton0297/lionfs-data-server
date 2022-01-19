import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;

public class DataServer {

	static final int MAX_CONNECTIONS = 10;
	static Queue<Socket> queue = new LinkedList<>();
	static Thread[] threads = new Thread[MAX_CONNECTIONS];
	
	public static void main(String[] args) {
		
		ServerSocket server = null;
		Socket client = null;
		
		try {

			// Start data node server
			server = new ServerSocket(Integer.parseInt(args[0]));
			
			System.out.println("Data Server START: Port " + args[0]);
			
			// Main while loop
			boolean listen = true;
			int id = 0;
			int threadCount = 0;
			while(listen) {
				
				// Get client socket
				client = server.accept();
				
				// Add socket to queue
				if(threadCount < 10) {
					queue.add(client);
					System.out.println("Socket #" + id + " CONNECT");
				} else {
					System.out.println("Too many connections.");
				}
				
				// Get client action
				String action = LionFSUtil.readString(client);
				
				// Process next socket
				Socket s = queue.poll();
//				System.out.println("thread count=" + threadCount);
				if(s != null && threadCount < threads.length) {
					System.out.println("Thread START for Socket " + id);
					Thread t = new Thread(new Worker(id, s, action));
					threads[id++ % MAX_CONNECTIONS] = t;
					t.start();
					threadCount++;
				}

				// Update threads
				for(int i = 0; i < threads.length; i++) {
					Thread t = threads[i];
					if(t != null) {
						if(t.getState() == Thread.State.TERMINATED) {
							threads[i] = null;
							threadCount--;
							System.out.println("Thread TERMINATED");
						}
					}
				}
				
				// Clean out temp folder
				File[] tempFiles = new File("temp/").listFiles();
				for(File f : tempFiles) {
					if(!f.isDirectory())
						f.delete();
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(server != null) server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}

class Worker implements Runnable {
	
	private static final String FS_PATH = "fs-files";
	private int id;
	private Socket socket;
	private FSAction action;

	public enum FSAction {
		GET_FILES_BY_USER,
		RETRIEVE,
		STORE,
		REMOVE
	}
	
	public Worker(int id, Socket socket, String action) {
		this.id = id;
		this.socket = socket;
		action = action.trim().toUpperCase();
		if(action.equals("GET_FILES_BY_USER")) {
			this.action = FSAction.GET_FILES_BY_USER;
		} else if(action.equals("RETRIEVE")) {
			this.action = FSAction.RETRIEVE;
		} else if(action.equals("STORE")) {
			this.action = FSAction.STORE;
		} else if(action.equals("REMOVE")) {
			this.action = FSAction.REMOVE;
		}
		File fs = new File(FS_PATH);
		if(!fs.exists()) {
			fs.mkdirs();
		}
	}
	
	public void run() {
	
		System.out.println("Thread " + this.id + " running...");

		try {

			// Read username
			String username = LionFSUtil.readString(socket);

			if(action == FSAction.GET_FILES_BY_USER) {

				System.out.println(username + " initiated action GET_FILES_BY_USER");

				// Get list of files from user dir
				File userDir = new File(FS_PATH + "/" + username);
				File[] userFiles = null;
				if(userDir.exists()) {
					userFiles = userDir.listFiles();
				}
				
				if(userFiles != null) {

					// Write file count
					LionFSUtil.writeInt(socket, userFiles.length);
					
					// Write file names
					for(int i = 0; i < userFiles.length; i++) {
						LionFSUtil.writeString(socket, userFiles[i].getName());
					} 
					
				}

			} else if(action == FSAction.STORE) {

				System.out.println(username + " initiated action STORE");

				// Check if username dir exists
				File userDir = new File(FS_PATH + "/" + username);
				if(!userDir.exists()) {
					userDir.mkdir();
				}
				
				// Read file count
				int fileCount = LionFSUtil.readInt(socket);
				
				// Read files and save in file system
				for(int i = 0; i < fileCount; i++) {
					String name = LionFSUtil.readString(socket);
					int size = LionFSUtil.readInt(socket);
					byte[] fileb = LionFSUtil.readBytes(socket, size);
					File outFile = new File(FS_PATH + "/" + username + "/" + name);
					LionFSUtil.writeBytesToFile(fileb, outFile);
				}
				
				System.out.println("Finished writing files.");	
		
			} else if(action == FSAction.RETRIEVE) {
				
				System.out.println(username + " initiated action RETRIEVE");

				// Read file count
				int fileCount = LionFSUtil.readInt(socket);
				
				for(int i = 0; i < fileCount; i++) {
					
					// Read file name
					String fileName = LionFSUtil.readString(socket);
					
					// Get file
					File file = new File(FS_PATH + "/" + username + "/" + fileName);
					
					// Write file
					LionFSUtil.writeFile(socket, file);
				}

			} else if(action == FSAction.REMOVE) {
				
				System.out.println(username + " initiated action RETRIEVE");
				
				// Read file name
				String fileName = LionFSUtil.readString(socket);
				
				// Get file
				File file = new File(FS_PATH + "/" + username + "/" + fileName);
				
				// Remove file
				if(file.exists() && !file.isDirectory()) {
					file.delete();
				}
				
			}
			
			System.out.println("Action COMPLETE");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(socket != null) socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
