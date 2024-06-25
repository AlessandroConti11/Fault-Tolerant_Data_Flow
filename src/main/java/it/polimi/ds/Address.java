package it.polimi.ds;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;

import org.javatuples.Pair;
import io.github.cdimascio.dotenv.Dotenv;
import it.polimi.ds.proto.ProtoAddress;

public class Address {

    public static Address getOwnAddress() throws SocketException {
        Dotenv dotenv = Dotenv.load();
        String inetIface = dotenv.get("INET_IFACE");
        if (inetIface == null) {
            System.err.println("INET_IFACE environment variable not set");
            System.exit(1);
        }

        NetworkInterface eth0 = NetworkInterface.getByName(inetIface);

        for (InterfaceAddress address : eth0.getInterfaceAddresses()) {
            InetAddress inetAddress = address.getAddress();
            if (inetAddress instanceof Inet4Address) {
                int prefixLength = address.getNetworkPrefixLength();
                return new Address(inetAddress.getHostAddress(), prefixLength, 0);
            }
        }

        throw new RuntimeException("No address found");
    }

    private String host;
    private int port;
    private int mask;

    public Address(String host) {
        this(host, 24, 0);
    }

    public Address(Socket socket) throws SocketException {
        this(socket.getInetAddress().getHostAddress(),
                NetworkInterface.getByInetAddress(socket.getInetAddress())
                        .getInterfaceAddresses().stream().filter(ia -> ia.getAddress().equals(socket.getInetAddress()))
                        .findFirst().get().getNetworkPrefixLength(),
                socket.getPort());
    }

    public Address(String host, int mask, int port) {
        this.host = host;
        this.port = port;
        this.mask = mask;
    }

    public Address(ProtoAddress node) {
        this(node.getIp(), node.getMask(), node.getPort());
    }

    public int getMask() {
        return mask;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Address withPort(int port) {
        return new Address(host, mask, port);
    }

    public ProtoAddress toProto() {
        return ProtoAddress.newBuilder().setIp(host).setMask(mask).setPort(port).build();
    }

    public Optional<Address> getNext() {
        assert mask > 0 && mask < 32;

        long max = 1 << (32 - mask);
        int[] parts = Arrays.stream(host.split("\\.")).mapToInt(Integer::parseInt).toArray();
        long ip = IntStream.range(0, 4).mapToLong(i -> parts[i] << ((3 - i) * 8)).sum() + 1;

        if ((ip & (max - 1)) != 0) {
            return Optional.of(new Address(String.format("%d.%d.%d.%d", (ip >> 24) & 0xff, (ip >> 16) & 0xff,
                    (ip >> 8) & 0xff, ip & 0xff), mask, port));
        } else {
            return Optional.empty();
        }
    }

    public Address getBroadcast() {
        int[] parts = Arrays.stream(host.split("\\.")).mapToInt(Integer::parseInt).toArray();
        long ip = IntStream.range(0, 4).mapToLong(i -> parts[i] << ((3 - i) * 8)).sum();
        ip = (ip | ((1 << (32 - mask)) - 1)) & 0xffffffffL;
        return new Address(String.format("%d.%d.%d.%d", (ip >> 24) & 0xff, (ip >> 16) & 0xff, (ip >> 8) & 0xff,
                ip & 0xff), mask, port);
    }

    public Address getNetwork() {
        int[] parts = Arrays.stream(host.split("\\.")).mapToInt(Integer::parseInt).toArray();
        long ip = IntStream.range(0, 4).mapToLong(i -> parts[i] << ((3 - i) * 8)).sum();
        ip = ip & (0xffffffffL << (32 - mask));
        return new Address(String.format("%d.%d.%d.%d", (ip >> 24) & 0xff, (ip >> 16) & 0xff, (ip >> 8) & 0xff,
                ip & 0xff), mask, 0);
    }

    @Override
    public String toString() {
        return String.format("%s/%d:%d", host, mask, port);
    }

    public static Pair<Address, String> fromString(String address) {
        String[] parts = address.split("/");

        String[] hostPort = parts[1].split(":");
        int mask = Integer.parseInt(hostPort[0]);

        int res = 0;
        int i = 0;
        for (i = 0; i < hostPort[1].length() && hostPort[1].charAt(i) != ' '; i++) {
            res = res * 10 + (hostPort[1].charAt(i) - '0');

        }

        int port = res;

        return new Pair<>(new Address(parts[0], mask, port),
                address.substring(parts[0].length() + 1 + hostPort[0].length() + 1 + i));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Address)) {
            return false;
        }
        Address other = (Address) obj;
        return host.equals(other.host) && port == other.port && mask == other.mask;
    }

    @Override
    public int hashCode() {
        int result = (int) (host.hashCode() ^ (host.hashCode() >>> 32));
        result = 31 * result + port;
        result = 31 * result + mask;
        return result;
    }

}
