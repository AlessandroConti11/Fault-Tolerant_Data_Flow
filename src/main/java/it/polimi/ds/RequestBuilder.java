package it.polimi.ds;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import it.polimi.ds.proto.AllocateNodeManagerRequest;
import it.polimi.ds.proto.AllocateNodeManagerResponse;
import it.polimi.ds.proto.AllocationRequest;
import it.polimi.ds.proto.AllocationResponse;
import it.polimi.ds.proto.ClientRequest;
import it.polimi.ds.proto.CloseRequest;
import it.polimi.ds.proto.CloseResponse;
import it.polimi.ds.proto.Data;
import it.polimi.ds.proto.DataRequest;
import it.polimi.ds.proto.DataResponse;
import it.polimi.ds.proto.Role;

import org.javatuples.Pair;

public class RequestBuilder {
    private final static ExecutorService exe = Executors.newCachedThreadPool();

    class Request {
        Node coordinator;
        Vector<Future<DataResponse>> responses = new Vector<>();

        private Request(Node coord) {
            this.coordinator = coord;
        }

        public Vector<DataResponse> getResponses() {
            return responses.stream().map(fut -> {
                try {
                    return fut.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                return null;
            }).collect(Collectors.toCollection(Vector::new));
        }

        public void sendData(List<Pair<Integer, Integer>> data) throws IOException {
            coordinator.send(ClientRequest.newBuilder()
                    .setDataRequest(DataRequest.newBuilder()
                            .setTaskId(-1)
                            .setSourceRole(Role.CLIENT)
                            .addAllData(data.stream()
                                    .map(d -> Data.newBuilder()
                                            .setKey(d.getValue0())
                                            .setValue(d.getValue1())
                                            .build())
                                    .collect(Collectors.toList()))
                            .build())
                    .build());

            responses.add(exe.submit(() -> {
                try {
                    return coordinator.receive(DataResponse.class);
                } catch (Throwable e) {
                    e.printStackTrace();

                    return null;
                }
            }));
        }

        public void close() throws IOException {
            coordinator.send(ClientRequest.newBuilder().setCloseRequest(CloseRequest.newBuilder().build()).build());
            coordinator.receive(CloseResponse.class);
            coordinator.close();
        }

    }

    Vector<Address> allocators = new Vector<>();
    ByteString program;
    int allocations;

    public RequestBuilder newBuilder() {
        return new RequestBuilder();
    }

    public RequestBuilder setProgram(ByteString program) {
        this.program = program;
        return this;
    }

    public RequestBuilder setAllocations(int allocations) {
        this.allocations = allocations;
        return this;
    }

    public RequestBuilder addAllocator(Address a) {
        this.allocators.add(a);
        return this;
    }

    public RequestBuilder addAllocators(List<Address> a) {
        this.allocators.addAll(a);
        return this;
    }

    public Request allocate() {
        System.out.println("Allocating resources " + allocators.get(0));
        Address coordinator_address = null;
        try {
            Node allocator = new Node(allocators.get(0));
            allocator.send(AllocateNodeManagerRequest.newBuilder()
                    .setCoordinator(true)
                    .build());

            var alloc_resp = allocator.receive(AllocateNodeManagerResponse.class);
            allocator.close();
            coordinator_address = new Address(alloc_resp.getAddress());
        } catch (IOException e) {
            System.err.println("Can't allocate resources -- " + e.getMessage());
            return null;
        }

        Node coordinator = null;
        try {
            coordinator = new Node(coordinator_address);
            coordinator.send(AllocationRequest.newBuilder()
                    .addAllAllocators(allocators.stream().map(a -> a.toProto()).collect(Collectors.toList()))
                    .setNumberOfAllocations(allocations)
                    .setRawProgram(program)
                    .build());

            var allocation_resp = coordinator.receive(AllocationResponse.class);
            switch (allocation_resp.getCode()) {
                case INVALID_PROGRAM:
                    throw new RuntimeException("The provided program is malformed -- " + allocation_resp.getCode());
                case NOT_ENOUGH_RESOURCES:
                    throw new RuntimeException("There is not enough resources -- " + allocation_resp.getCode());

                case OK:
                    break;

                default:
                    throw new RuntimeException("Unexpected response from coordinator -- " + allocation_resp.getCode());
            }
        } catch (IOException e) {
            System.err.println("Can't create execution plan on the coordinator -- " + e.getMessage());
            return null;
        }

        return new Request(coordinator);
    }

}
