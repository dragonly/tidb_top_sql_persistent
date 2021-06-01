// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_RESOURCE_USAGE_AGENT_COLLECT_CPU_TIME: ::grpcio::Method<super::agent::CollectCPUTimeRequest, super::agent::CollectCPUTimeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ClientStreaming,
    name: "/ResourceUsageAgent/CollectCPUTime",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct ResourceUsageAgentClient {
    client: ::grpcio::Client,
}

impl ResourceUsageAgentClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ResourceUsageAgentClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn collect_cpu_time_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientCStreamSender<super::agent::CollectCPUTimeRequest>, ::grpcio::ClientCStreamReceiver<super::agent::CollectCPUTimeResponse>)> {
        self.client.client_streaming(&METHOD_RESOURCE_USAGE_AGENT_COLLECT_CPU_TIME, opt)
    }

    pub fn collect_cpu_time(&self) -> ::grpcio::Result<(::grpcio::ClientCStreamSender<super::agent::CollectCPUTimeRequest>, ::grpcio::ClientCStreamReceiver<super::agent::CollectCPUTimeResponse>)> {
        self.collect_cpu_time_opt(::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Output = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait ResourceUsageAgent {
    fn collect_cpu_time(&mut self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::agent::CollectCPUTimeRequest>, sink: ::grpcio::ClientStreamingSink<super::agent::CollectCPUTimeResponse>);
}

pub fn create_resource_usage_agent<S: ResourceUsageAgent + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_client_streaming_handler(&METHOD_RESOURCE_USAGE_AGENT_COLLECT_CPU_TIME, move |ctx, req, resp| {
        instance.collect_cpu_time(ctx, req, resp)
    });
    builder.build()
}
