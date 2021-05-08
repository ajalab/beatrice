mod collections;
pub mod configuration;
mod model;
mod store;

use self::{model::Row, store::Store};
pub use beatrice_proto::beatrice::beatrice_server::BeatriceStateMachine;
use beatrice_proto::beatrice::{
    beatrice_server::Beatrice, DeleteRequest, DeleteResponse, FlushRequest, FlushResponse,
    GetRequest, GetResponse, PutRequest, PutResponse,
};
use bytes::Bytes;
use std::{convert::TryFrom, time::SystemTime};
use tonic::{Response, Status};

pub struct BeatriceState {
    store: Store,
}

impl BeatriceState {
    pub fn new() -> Self {
        Self {
            store: Store::new(2048),
        }
    }
}

#[repc::async_trait]
impl Beatrice for BeatriceState {
    async fn put(&mut self, req: PutRequest) -> Result<Response<PutResponse>, Status> {
        let row = Row::new(Bytes::from(req.row));
        let timestamp = match req.timestamp {
            0 => get_current_timestamp_millis().map_err(|e| {
                Status::internal(format!("failed to get current time: error={:?}", e))
            })?,
            t => t,
        };
        let val = Bytes::from(req.value);
        self.store.put(row, timestamp, val);

        Ok(Response::new(PutResponse {}))
    }

    async fn get(&mut self, req: GetRequest) -> Result<Response<GetResponse>, Status> {
        let row = Row::new(Bytes::from(req.row));
        self.store
            .get_latest(&row)
            .map(|(k, v)| {
                Response::new(GetResponse {
                    timestamp: k.timestamp(),
                    value: v.to_vec(),
                })
            })
            .ok_or_else(|| Status::not_found("not found"))
    }

    async fn delete(&mut self, req: DeleteRequest) -> Result<Response<DeleteResponse>, Status> {
        let row = Row::new(Bytes::from(req.row));
        let timestamp = match req.timestamp {
            0 => get_current_timestamp_millis().map_err(|e| {
                Status::internal(format!("failed to get current time: error={:?}", e))
            })?,
            t => t,
        };
        self.store.delete(row, timestamp);

        Ok(Response::new(DeleteResponse {}))
    }

    async fn flush(&mut self, req: FlushRequest) -> Result<Response<FlushResponse>, Status> {
        self.store.flush(req.cache);
        Ok(Response::new(FlushResponse {}))
    }
}

fn get_current_timestamp_millis() -> anyhow::Result<u64> {
    let d = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    let millis = u64::try_from(d.as_millis())?;
    Ok(millis)
}
