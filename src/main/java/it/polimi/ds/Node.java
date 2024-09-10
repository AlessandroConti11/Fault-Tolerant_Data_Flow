package it.polimi.ds;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

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
        byte[] data = message.toByteArray();
        byte[] len_plus_data = new byte[4 + data.length];

        ByteBuffer.wrap(len_plus_data).putInt(data.length);
        for (int i = 4; i < len_plus_data.length; i++) {
            len_plus_data[i] = data[i - 4];
        }

        out.write(len_plus_data);
    }

    public <T extends GeneratedMessageV3> T receive(Class<T> clazz, int timeout)
            throws SocketTimeoutException, IOException {
        conn.setSoTimeout(timeout);
        T ret = null;
        try {
            ret = receive(clazz);
        } catch (SocketTimeoutException e) {
            conn.setSoTimeout(0);
            throw e;
        }
        return ret;
    }

    public <T extends GeneratedMessageV3> T receive(Class<T> clazz) throws IOException {
        byte[] len_bytes = new byte[4];
        if (in.read(len_bytes) != 4) {
            throw new IOException("Unable to read the message length");
        }

        final int len = ByteBuffer.wrap(len_bytes).getInt();
        byte[] msg_bytes = new byte[len];
        int read = in.read(msg_bytes);

        if (read != len) {
            throw new IOException("Unable to read the message length, read " + read + " needed " + len);
        }

        T t = null;
        try {
            t = (T) clazz.getMethod("parseFrom", ByteBuffer.class).invoke(null, ByteBuffer.wrap(msg_bytes));

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

    public <T extends GeneratedMessageV3> T nonBlockReceive(Class<T> clazz) throws IOException {
        SocketChannel channel = conn.getChannel();
        byte[] len_bytes = new byte[4];
        int len_bytes_read = channel.read(ByteBuffer.wrap(len_bytes));
        if (len_bytes_read == -1) {
            throw new ClosedChannelException();
        }
        if (len_bytes_read != 4) {
            throw new IOException("Unable to read the message length, expected 4 but got: " + len_bytes_read);
        }

        final int len = ByteBuffer.wrap(len_bytes).getInt();
        byte[] msg_bytes = new byte[len];
        int read = channel.read(ByteBuffer.wrap(msg_bytes));

        if (read != len) {
            throw new IOException("Unable to read the message length, read " + read + " needed " + len);
        }

        T t = null;
        try {
            t = (T) clazz.getMethod("parseFrom", ByteBuffer.class).invoke(null, ByteBuffer.wrap(msg_bytes));

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
