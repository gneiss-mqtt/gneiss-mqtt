/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![cfg_attr(not(any(feature = "tokio", feature = "threaded")), allow(dead_code))]
#![cfg_attr(all(feature = "testing", not(test)), allow(dead_code, unused_imports, unused_macros))]
#![cfg_attr(feature = "strict", deny(warnings))]

pub mod model;
pub mod asynchronous;
pub mod synchronous;
mod error;