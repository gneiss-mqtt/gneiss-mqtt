/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use gneiss_mqtt::client::AsyncClientHandle;
use crate::client::*;

pub(crate) struct TokioClientProtocolAdapter {
    protocol_client: AsyncClientHandle,
    runtime: tokio::runtime::Handle,
}

impl ProtocolAdapter for TokioClientProtocolAdapter {
    fn subscribe(&self, options: SubscribeOptions) -> RequestResponseResult<()> {
        Ok(())
    }

    fn unsubscribe(&self, options: UnsubscribeOptions) -> RequestResponseResult<()> {
        Ok(())
    }

    fn publish(&self, options: PublishOptions) -> RequestResponseResult<()> {
        Ok(())
    }
}