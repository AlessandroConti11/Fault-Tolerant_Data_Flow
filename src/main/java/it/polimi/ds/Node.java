package it.polimi.ds;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.SocketTimeoutException;

import com.google.protobuf.GeneratedMessageV3;

public class Node {
    private Socket conn;

    private OutputStream out;
    private InputStream in;

    public Node(Socket socket) {
        this.conn = socket;
        try {
            this.out = socket.getOutputStream();
            this.in = socket.getInputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Node(Address address) {
        try {
            this.conn = new Socket(address.getHost(), address.getPort());
            this.out = conn.getOutputStream();
            this.in = conn.getInputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void send(GeneratedMessageV3 message) throws IOException {
        message.writeDelimitedTo(out);
    }

    public <T extends GeneratedMessageV3> T receive(Class<T> clazz, int timeout)
            throws SocketTimeoutException, IOException {
        conn.setSoTimeout(timeout);
        T ret = receive(clazz);
        conn.setSoTimeout(0);
        return ret;
    }

    public <T extends GeneratedMessageV3> T receive(Class<T> clazz) throws IOException {
        T t = null;
        try {
            t = (T) clazz.getMethod("parseDelimitedFrom", InputStream.class).invoke(null, in);
        } catch (IllegalAccessException | NoSuchMethodException | SecurityException e) {
            System.err.println("Error while parsing message -- " + clazz.getName() + " -- " + e.getMessage());
            // Unreachable
            e.printStackTrace();
            System.exit(1);
        } catch (InvocationTargetException e) {
            throw (IOException) e.getCause();
        }

        return t;
    }

    public boolean isAlive() throws IOException {
        return conn.isConnected();

    }

    public void close() throws IOException {
        conn.close();
    }
}
