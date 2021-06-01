// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package protobuf

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// TopSQLAgentClient is the client API for TopSQLAgent service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TopSQLAgentClient interface {
	// CollectCPUTime is called periodically (e.g. per minute) to save the in-memory TopSQL records
	CollectCPUTime(ctx context.Context, opts ...grpc.CallOption) (TopSQLAgent_CollectCPUTimeClient, error)
}

type topSQLAgentClient struct {
	cc grpc.ClientConnInterface
}

func NewTopSQLAgentClient(cc grpc.ClientConnInterface) TopSQLAgentClient {
	return &topSQLAgentClient{cc}
}

func (c *topSQLAgentClient) CollectCPUTime(ctx context.Context, opts ...grpc.CallOption) (TopSQLAgent_CollectCPUTimeClient, error) {
	stream, err := c.cc.NewStream(ctx, &TopSQLAgent_ServiceDesc.Streams[0], "/TopSQLAgent/CollectCPUTime", opts...)
	if err != nil {
		return nil, err
	}
	x := &topSQLAgentCollectCPUTimeClient{stream}
	return x, nil
}

type TopSQLAgent_CollectCPUTimeClient interface {
	Send(*CollectCPUTimeRequest) error
	CloseAndRecv() (*CollectCPUTimeResponse, error)
	grpc.ClientStream
}

type topSQLAgentCollectCPUTimeClient struct {
	grpc.ClientStream
}

func (x *topSQLAgentCollectCPUTimeClient) Send(m *CollectCPUTimeRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *topSQLAgentCollectCPUTimeClient) CloseAndRecv() (*CollectCPUTimeResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(CollectCPUTimeResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// TopSQLAgentServer is the server API for TopSQLAgent service.
// All implementations must embed UnimplementedTopSQLAgentServer
// for forward compatibility
type TopSQLAgentServer interface {
	// CollectCPUTime is called periodically (e.g. per minute) to save the in-memory TopSQL records
	CollectCPUTime(TopSQLAgent_CollectCPUTimeServer) error
	mustEmbedUnimplementedTopSQLAgentServer()
}

// UnimplementedTopSQLAgentServer must be embedded to have forward compatible implementations.
type UnimplementedTopSQLAgentServer struct {
}

func (UnimplementedTopSQLAgentServer) CollectCPUTime(TopSQLAgent_CollectCPUTimeServer) error {
	return status.Errorf(codes.Unimplemented, "method CollectCPUTime not implemented")
}
func (UnimplementedTopSQLAgentServer) mustEmbedUnimplementedTopSQLAgentServer() {}

// UnsafeTopSQLAgentServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TopSQLAgentServer will
// result in compilation errors.
type UnsafeTopSQLAgentServer interface {
	mustEmbedUnimplementedTopSQLAgentServer()
}

func RegisterTopSQLAgentServer(s grpc.ServiceRegistrar, srv TopSQLAgentServer) {
	s.RegisterService(&TopSQLAgent_ServiceDesc, srv)
}

func _TopSQLAgent_CollectCPUTime_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(TopSQLAgentServer).CollectCPUTime(&topSQLAgentCollectCPUTimeServer{stream})
}

type TopSQLAgent_CollectCPUTimeServer interface {
	SendAndClose(*CollectCPUTimeResponse) error
	Recv() (*CollectCPUTimeRequest, error)
	grpc.ServerStream
}

type topSQLAgentCollectCPUTimeServer struct {
	grpc.ServerStream
}

func (x *topSQLAgentCollectCPUTimeServer) SendAndClose(m *CollectCPUTimeResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *topSQLAgentCollectCPUTimeServer) Recv() (*CollectCPUTimeRequest, error) {
	m := new(CollectCPUTimeRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// TopSQLAgent_ServiceDesc is the grpc.ServiceDesc for TopSQLAgent service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TopSQLAgent_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "TopSQLAgent",
	HandlerType: (*TopSQLAgentServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "CollectCPUTime",
			Handler:       _TopSQLAgent_CollectCPUTime_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "agent.proto",
}
