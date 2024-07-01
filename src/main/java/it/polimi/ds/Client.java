package it.polimi.ds;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.RequestBuilder.Request;

import org.javatuples.Pair;

public class Client {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Request request = new RequestBuilder()
                .setAllocations(1)
                .setProgram(ByteString.copyFromUtf8("map;add;6"))
                .addAllocators(Arrays.asList(args).stream()
                        .map(Address::fromStringIp)
                        .map(a -> a.getValue0().withPort(Allocator.PORT))
                        .collect(Collectors.toList()))
                .allocate();

        List<Pair<Integer, Integer>> data = new ArrayList<>();

        data.add(new Pair<>(1, 1));
        data.add(new Pair<>(2, 2));

        request.sendData(data);
        // request.sendData(ByteString.copyFromUtf8("Hello World!"));
        // request.sendData(ByteString.copyFromUtf8("Hello World!"));

        request.getResponses().forEach(System.out::println);
        request.close();

    }
}
