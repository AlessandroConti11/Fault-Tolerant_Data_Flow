package it.polimi.ds;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

import it.polimi.ds.RequestBuilder.Request;
import org.javatuples.Pair;

public class Client {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Request request = new RequestBuilder()
                .setAllocations(1)
                .setProgram(ByteString.copyFromUtf8("Hello World!"))
                .addAllocator(new Address("192.168.1.55", 24, Allocator.PORT))
                .allocate();

        List<Pair<Integer, Integer>> data = new ArrayList<>();

        request.sendData(data);
        // request.sendData(ByteString.copyFromUtf8("Hello World!"));
        // request.sendData(ByteString.copyFromUtf8("Hello World!"));

        request.getResponses().forEach(System.out::println);
        request.close();

    }
}
