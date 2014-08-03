package resa.migrate;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by ding on 14-7-31.
 */
public class FileServer extends Thread {
    private final int port;
    private ServerSocket serverSocket;
    private final Path root;
    private volatile boolean stop = false;

    public FileServer(int port, String base) {
        super("File server");
        setDaemon(true);
        root = Paths.get(base);
        this.port = port;
    }

    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started at port " + port);
        } catch (Exception e) {
            System.err.println("Port " + port + " already in use.");
            return;
        }
        while (!stop) {
            try {
                Socket clientSocket = serverSocket.accept();
                // System.out.println("Accepted connection : " + clientSocket);
                Thread t = new Thread(new Connection(clientSocket));
                t.start();
            } catch (Exception e) {
                System.err.println("Error in connection attempt.");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        FileServer server = new FileServer(Integer.parseInt(args[0]), args[1]);
        server.start();
        server.join();
    }

    private class Connection implements Runnable {
        private Socket clientSocket;

        public Connection(Socket client) {
            this.clientSocket = client;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                String clientSelection;
                while ((clientSelection = in.readLine()) != null) {
                    switch (clientSelection) {
                        case "write":
                            receiveFile();
                            break;
                        case "read":
                            String outGoingFileName;
                            while ((outGoingFileName = in.readLine()) != null) {
                                sendFile(outGoingFileName);
                            }
                            break;
                        default:
                            System.out.println("Incorrect command received.");
                            break;
                    }
                    break;
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        public void receiveFile() {
            try (DataInputStream clientData = new DataInputStream(clientSocket.getInputStream())) {
                String fileName = clientData.readUTF();
                OutputStream output = Files.newOutputStream(root.resolve(fileName));
                long size = clientData.readLong();
                if (size == -1) {
                    size = Long.MAX_VALUE;
                }
                byte[] buffer = new byte[1024];
                int bytesRead;
                while (size > 0 && (bytesRead = clientData.read(buffer, 0, (int) Math.min(buffer.length, size))) != -1) {
                    output.write(buffer, 0, bytesRead);
                    size -= bytesRead;
                }
                output.close();
                System.out.println("File " + fileName + " received from client.");
            } catch (IOException ex) {
                System.err.println("Client error. Connection closed.");
            }
        }

        public void sendFile(String fileName) {
            try {
                //handle file read
                File myFile = root.resolve(fileName).toFile();
                byte[] mybytearray = new byte[(int) myFile.length()];

                FileInputStream fis = new FileInputStream(myFile);
                BufferedInputStream bis = new BufferedInputStream(fis);
                //bis.read(mybytearray, 0, mybytearray.length);

                DataInputStream dis = new DataInputStream(bis);
                dis.readFully(mybytearray, 0, mybytearray.length);

                //handle file send over socket
                OutputStream os = clientSocket.getOutputStream();

                //Sending file name and file size to the server
                DataOutputStream dos = new DataOutputStream(os);
                dos.writeUTF(myFile.getName());
                dos.writeLong(mybytearray.length);
                dos.write(mybytearray, 0, mybytearray.length);
                dos.flush();
                System.out.println("File " + fileName + " sent to client.");
            } catch (Exception e) {
                System.err.println("File does not exist!");
            }
        }
    }
}
