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
                .setAllocations(2)
                .setProgram(ByteString.copyFromUtf8("filter;not_equal;55\n" +
                        "change_key;add;70\n" +
                        "map;add;13\n" +
                        "filter;lower_or_equal;93\n" +
                        "change_key;add;75\n" +
                        "filter;not_equal;11\n" +
                        "map;mult;77\n" +
                        "filter;not_equal;19\n" +
                        "change_key;add;75" +
                    ""))
                .addAllocators(Arrays.asList(args).stream()
                        .map(Address::fromStringIp)
                        .map(a -> a.getValue0().withPort(Allocator.PORT))
                        .collect(Collectors.toList()))
                .allocate();

        List<Pair<Integer, Integer>> data = new ArrayList<>();

        data.add(new Pair<>(1, 1));
        data.add(new Pair<>(2, 2));

        request.sendData(data);
        request.sendData(data);
        // request.sendData(data);
        // request.sendData(data);

        request.getResponses().forEach(System.out::println);
        request.close();

    }
}
