/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
A module with types for handling topic alias resolution, both outbound and inbound.  The included
outbound resolvers should be sufficient for most use cases, but this module also includes a trait
that allows for custom resolution implementations to be injected into a client.
*/

#[cfg(test)]
use assert_matches::assert_matches;

use crate::error::{MqttError, MqttResult};

use log::*;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Captures the outcome of an outbound topic alias resolution attempt.  Outbound topic alias
/// resolution is performed as the client encodes a Publish packet before writing it to the
/// socket.
#[derive(Default, Copy, Clone)]
#[cfg_attr(feature = "testing", derive(Debug, Eq, PartialEq))]
pub struct OutboundAliasResolution {
    /// Should the client use an empty topic during packet encoding?
    pub skip_topic : bool,

    /// What topic alias to use, if any, during packet encoding.
    pub alias : Option<u16>,
}

/// Trait allowing users to implement their own outbound topic alias resolver.
pub trait OutboundAliasResolver : Send {

    /// Called by the client after receiving a successful Connack from the broker.  Informs the
    /// resolver of the maximum allowed alias.  If zero is passed in, then topic aliases are
    /// forbidden.
    fn reset_for_new_connection(&mut self, max_aliases : u16);

    /// Attempts to resolve a topic alias, updating the resolver's internal state while doing
    /// so.
    ///
    /// `alias` - current topic alias value in the Publish packet
    /// `topic` - current topic value in the Publish packet
    ///
    /// Output: the resolver's topic alias resolution outcome
    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution;
}

type OutboundAliasResolverFactoryReturnType = Box<dyn OutboundAliasResolver + Send>;

/// Signature for factory functions that build new outbound alias resolvers
pub type OutboundAliasResolverFactoryFn = Arc<dyn Fn() -> OutboundAliasResolverFactoryReturnType + Send + Sync>;

/// Static factory for constructing all of the outbound topic alias resolver variants that are
/// included in the base library.
pub struct OutboundAliasResolverFactory {
}

impl OutboundAliasResolverFactory {

    /// Creates a new outbound topic alias resolver that never uses topic aliases.
    pub fn new_null_factory() -> OutboundAliasResolverFactoryFn {
        Arc::new(|| { Box::new( NullOutboundAliasResolver::new()) })
    }

    /// An outbound topic alias resolver that only uses aliases supplied by the user in the Publish
    /// packet.  Note that there are multiple reasons why a user-supplied alias might not be
    /// usable (too big, not-yet-seen, etc...).  In these cases, the resolver does not use a topic
    /// alias.
    pub fn new_manual_factory() -> OutboundAliasResolverFactoryFn {
        Arc::new(|| { Box::new( ManualOutboundAliasResolver::new()) })
    }

    /// An outbound topic alias resolver that uses an LRU cache to automatically make topic
    /// aliasing choices.  This is likely to give good (topic compression) performance in normal
    /// use cases.  Sub-optimal performance could occur if the working set of topics substantially
    /// exceeds what the broker is allowing, or if there is a highly skewed distribution where
    /// commonly-used topics are very short and uncommon topics are very long.
    pub fn new_lru_factory(maximum_alias_value : u16) -> OutboundAliasResolverFactoryFn {
        Arc::new(move || { Box::new( LruOutboundAliasResolver::new(maximum_alias_value)) })
    }
}

struct NullOutboundAliasResolver {
}

impl NullOutboundAliasResolver {
    fn new() -> NullOutboundAliasResolver {
        NullOutboundAliasResolver {}
    }

    #[cfg(test)]
    fn get_maximum_alias_value(&self) -> u16 { 0 }
}

impl OutboundAliasResolver for NullOutboundAliasResolver {

    fn reset_for_new_connection(&mut self, _ : u16) {}

    fn resolve_and_apply_topic_alias(&mut self, _: &Option<u16>, _: &str) -> OutboundAliasResolution {
        OutboundAliasResolution {
            ..Default::default()
        }
    }
}


struct ManualOutboundAliasResolver {
    maximum_alias_value : u16,

    current_aliases : HashMap<u16, String>,
}

impl ManualOutboundAliasResolver {

    fn new() -> ManualOutboundAliasResolver {
        ManualOutboundAliasResolver {
            maximum_alias_value: 0,
            current_aliases : HashMap::new(),
        }
    }

    fn resolve_topic_alias(&self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        if let Some(alias_ref_value) = alias {
            let alias_value = *alias_ref_value;
            if let Some(existing_alias) = self.current_aliases.get(alias_ref_value) {
                if *existing_alias == *topic {
                    return OutboundAliasResolution {
                        skip_topic: true,
                        alias : Some(alias_value)
                    };
                }
            }

            if alias_value > 0 && alias_value < self.maximum_alias_value {
                return OutboundAliasResolution{
                    skip_topic: false,
                    alias: Some(alias_value)
                };
            }
        }

        OutboundAliasResolution { ..Default::default() }
    }

    #[cfg(test)]
    fn get_maximum_alias_value(&self) -> u16 { self.maximum_alias_value }
}


impl OutboundAliasResolver for ManualOutboundAliasResolver {

    fn reset_for_new_connection(&mut self, maximum_alias_value : u16) {
        self.maximum_alias_value = maximum_alias_value;
        self.current_aliases.clear();
    }

    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        let resolution = self.resolve_topic_alias(alias, topic);

        if let Some(resolved_alias) = resolution.alias {
            if !resolution.skip_topic {
                self.current_aliases.insert(resolved_alias, topic.to_string());
            }
        }

        resolution
    }
}

struct LruOutboundAliasResolver {
    current_maximum_alias_value: u16,
    maximum_alias_value : u16,

    cache : LruCache<String, u16>
}

impl LruOutboundAliasResolver {
    fn new(maximum_alias_value : u16) -> LruOutboundAliasResolver {
        LruOutboundAliasResolver {
            current_maximum_alias_value: 0,
            maximum_alias_value,
            cache : LruCache::new(NonZeroUsize::new(u16::max(1, maximum_alias_value) as usize).unwrap())
        }
    }

    fn resolve_topic_alias(&self, _: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        if self.current_maximum_alias_value == 0 {
            return OutboundAliasResolution{
                skip_topic: false,
                alias : None
            };
        }

        if let Some(cached_alias_value) = self.cache.peek(topic) {
            let alias_value = *cached_alias_value;

            return OutboundAliasResolution{
                skip_topic: true,
                alias : Some(alias_value)
            };
        }

        let mut alias_value : u16 = (self.cache.len() + 1) as u16;
        if alias_value > self.current_maximum_alias_value {
            if let Some((_, recycled_alias)) = self.cache.peek_lru() {
                alias_value = *recycled_alias;
            } else {
                panic!("Illegal state in LRU outbound topic alias resolver")
            }
        }

        OutboundAliasResolution{
            skip_topic: false,
            alias : Some(alias_value)
        }
    }

    #[cfg(test)]
    fn get_maximum_alias_value(&self) -> u16 { self.current_maximum_alias_value }
}

impl OutboundAliasResolver for LruOutboundAliasResolver {

    fn reset_for_new_connection(&mut self, maximum_alias_value : u16) {
        self.current_maximum_alias_value = u16::min(self.maximum_alias_value, maximum_alias_value);
        self.cache.clear();
    }

    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        let resolution = self.resolve_topic_alias(alias, topic);
        if resolution.alias.is_none() {
            return resolution;
        }

        if resolution.skip_topic {
            self.cache.promote(topic);
        } else {
            if self.cache.len() == self.current_maximum_alias_value as usize {
                self.cache.pop_lru();
            }

            let resolved_alias = resolution.alias.unwrap();
            self.cache.push(topic.to_string(), resolved_alias);
        }

        resolution
    }
}

pub(crate) struct InboundAliasResolver {
    maximum_alias_value: u16,

    current_aliases : HashMap<u16, String>
}

impl InboundAliasResolver {
    pub fn new(maximum_alias_value: u16) -> InboundAliasResolver {
        InboundAliasResolver {
            maximum_alias_value,
            current_aliases : HashMap::new()
        }
    }

    pub(crate) fn reset_for_new_connection(&mut self) {
        self.current_aliases.clear();
    }

    pub(crate) fn resolve_topic_alias(&mut self, alias: &Option<u16>, topic: &mut String) -> MqttResult<()> {
        if let Some(alias_value) = alias {
            if topic.is_empty() {
                if let Some(existing_topic) = self.current_aliases.get(alias_value) {
                    *topic = existing_topic.clone();
                    return Ok(());
                }

                error!("Topic Alias Resolution - zero length topic");
                return Err(MqttError::new_inbound_topic_alias_not_valid("No alias binding exists for topic-less publish"));
            }

            if *alias_value == 0 || *alias_value > self.maximum_alias_value {
                error!("Topic Alias Resolution - inbound alias out of range");
                return Err(MqttError::new_inbound_topic_alias_not_valid("Publish alias value out of negotiated range"));
            }

            self.current_aliases.insert(*alias_value, topic.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::alias::*;

    #[test]
    fn outbound_topic_alias_null() {
        let mut resolver = NullOutboundAliasResolver::new();

        resolver.reset_for_new_connection(20);
        assert_eq!(resolver.get_maximum_alias_value(), 0);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});

        resolver.reset_for_new_connection(10);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
    }

    #[test]
    fn outbound_topic_alias_manual_invalid() {
        let mut resolver = ManualOutboundAliasResolver::new();

        resolver.reset_for_new_connection(20);
        assert_eq!(resolver.get_maximum_alias_value(), 20);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(0), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(21), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(65535), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});

        resolver.reset_for_new_connection(10);

        assert_eq!(resolver.get_maximum_alias_value(), 10);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(0), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(11), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(65535), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
    }

    #[test]
    fn outbound_topic_alias_manual_success() {
        let mut resolver = ManualOutboundAliasResolver::new();

        resolver.reset_for_new_connection(20);
        assert_eq!(resolver.get_maximum_alias_value(), 20);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});

        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_manual_reset() {
        let mut resolver = ManualOutboundAliasResolver::new();

        resolver.reset_for_new_connection(20);
        assert_eq!(resolver.get_maximum_alias_value(), 20);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});

        resolver.reset_for_new_connection(10);

        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});

        assert_eq!(resolver.get_maximum_alias_value(), 10);
    }

    /*
     * lru topic sequence tests
     *  cache size of 2
     *  a, b, c, refer to distinct topics
     *  the 'r' suffix refers to expected alias reuse
     */
    #[test]
    fn outbound_topic_alias_lru_a_ar() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
    }

    #[test]
    fn outbound_topic_alias_lru_b_a_br() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_ar_br() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_c_br_cr_br_cr_a() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_c_a_cr_b() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_reset_a_b() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        resolver.reset_for_new_connection(2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_disabled_by_zero_maximum() {
        let mut resolver = LruOutboundAliasResolver::new(2);
        resolver.reset_for_new_connection(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        resolver.reset_for_new_connection(0);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
    }

    #[test]
    fn inbound_topic_alias_resolve_success() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();
        let mut topic2 = "topic2".to_string();

        assert!(resolver.resolve_topic_alias(&Some(1), &mut topic1).is_ok());
        assert_eq!(topic1, "topic1");

        assert!(resolver.resolve_topic_alias(&Some(10), &mut topic2).is_ok());
        assert_eq!(topic2, "topic2");

        let mut unresolved_topic1 = "".to_string();
        let mut unresolved_topic2 = "".to_string();

        assert!(resolver.resolve_topic_alias(&Some(1), &mut unresolved_topic1).is_ok());
        assert_eq!(unresolved_topic1, "topic1");

        assert!(resolver.resolve_topic_alias(&Some(10), &mut unresolved_topic2).is_ok());
        assert_eq!(unresolved_topic2, "topic2");
    }

    #[test]
    fn inbound_topic_alias_resolve_success_rebind_existing() {
        let mut resolver = InboundAliasResolver::new(2);

        let mut topic1 = "topic1".to_string();
        let mut topic3 = "topic3".to_string();

        assert!(resolver.resolve_topic_alias(&Some(1), &mut topic1).is_ok());
        assert_eq!(topic1, "topic1");

        assert!(resolver.resolve_topic_alias(&Some(1), &mut topic3).is_ok());
        assert_eq!(topic3, "topic3");

        let mut unresolved_topic3 = "".to_string();

        assert!(resolver.resolve_topic_alias(&Some(1), &mut unresolved_topic3).is_ok());
        assert_eq!(unresolved_topic3, "topic3");
    }


    #[test]
    fn inbound_topic_alias_resolve_failures() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();

        assert_matches!(resolver.resolve_topic_alias(&Some(0), &mut topic1), Err(MqttError::InboundTopicAliasNotValid(_)));
        assert_matches!(resolver.resolve_topic_alias(&Some(11), &mut topic1), Err(MqttError::InboundTopicAliasNotValid(_)));

        let mut empty_topic = "".to_string();
        assert_matches!(resolver.resolve_topic_alias(&Some(2), &mut empty_topic), Err(MqttError::InboundTopicAliasNotValid(_)));
    }

    #[test]
    fn inbound_topic_alias_resolve_reset() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();

        assert!(resolver.resolve_topic_alias(&Some(1), &mut topic1).is_ok());

        let mut empty_topic = "".to_string();
        assert!(resolver.resolve_topic_alias(&Some(1), &mut empty_topic).is_ok());
        assert_eq!(empty_topic, "topic1");

        resolver.reset_for_new_connection();
        empty_topic = "".to_string();
        assert_matches!(resolver.resolve_topic_alias(&Some(1), &mut empty_topic), Err(MqttError::InboundTopicAliasNotValid(_)));
    }
}

