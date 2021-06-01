mod protos;

use std::sync::Arc;

use grpcio::{ChannelBuilder, EnvBuilder};
use protos::agent;

fn main() {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect("localhost:23333");
}
