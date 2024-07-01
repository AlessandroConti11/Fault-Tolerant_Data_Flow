package it.polimi.ds;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
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
import org.javatuples.Pair;

public class RequestBuilder {
    class Request {
        Node coordinator;
        Vector<DataResponse> responses = new Vector<>();

        private Request(Node coord) {
            this.coordinator = coord;
        }

        public Vector<DataResponse> getResponses() {
            return responses;
        }

        public Object sendData(List<Pair<Integer, Integer>> data) throws IOException {
            coordinator.send(ClientRequest.newBuilder()
                    .setDataRequest(DataRequest.newBuilder()
                            .setTaskId(-1)
                            .addAllData(data.stream()
                                    .map(d -> Data.newBuilder()
                                            .setKey(d.getValue0())
                                            .setValue(d.getValue1())
                                            .build())
                                    .collect(Collectors.toList()))
                            .build())
                    .build());

            DataResponse response = coordinator.receive(DataResponse.class);
            responses.add(response);
            return response;
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

    public RequestBuilder addAllocators(Vector<Address> a) {
        this.allocators.addAll(a);
        return this;
    }

    public Request allocate() {
        System.out.println("Allocating resources" + allocators.get(0));
        Node allocator = new Node(allocators.get(0));
        Address coordinator_address = null;
        try {
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
        } catch (IOException e) {
            System.err.println("Can't create execution plan on the coordinator -- " + e.getMessage());
            return null;
        }

        return new Request(coordinator);
    }

}
