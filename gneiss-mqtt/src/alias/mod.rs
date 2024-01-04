/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
A module with types for handling topic alias resolution, both outbound and inbound.  The included
outbound resolvers should be sufficient for most use cases, but this module also includes a trait
that allows for custom resolution implementations to be injected into a client.
*/

extern crate log;
extern crate lru;

use crate::*;

use log::*;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;

#[derive(Default, Copy, Clone)]
#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
pub struct OutboundAliasResolution {
    pub skip_topic : bool,

    pub alias : Option<u16>,
}

pub trait OutboundAliasResolver : Send {
    fn get_maximum_alias_value(&self) -> u16;

    fn reset_for_new_connection(&mut self, max_aliases : u16);

    fn resolve_topic_alias(&self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution;

    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution;
}

impl<T: OutboundAliasResolver + ?Sized> OutboundAliasResolver for Box<T> {
    fn get_maximum_alias_value(&self) -> u16 { self.as_ref().get_maximum_alias_value() }

    fn reset_for_new_connection(&mut self, max_aliases : u16) { self.as_mut().reset_for_new_connection(max_aliases); }

    fn resolve_topic_alias(&self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        self.as_ref().resolve_topic_alias(alias, topic)
    }

    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        self.as_mut().resolve_and_apply_topic_alias(alias, topic)
    }
}

pub struct NullOutboundAliasResolver {

}

impl NullOutboundAliasResolver {
    pub fn new() -> NullOutboundAliasResolver {
        NullOutboundAliasResolver {}
    }
}

impl Default for NullOutboundAliasResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl OutboundAliasResolver for NullOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16 { 0 }

    fn reset_for_new_connection(&mut self, _ : u16) {}

    fn resolve_topic_alias(&self, _: &Option<u16>, _: &str) -> OutboundAliasResolution {
        OutboundAliasResolution {
            ..Default::default()
        }
    }

    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        self.resolve_topic_alias(alias, topic)
    }
}

pub struct ManualOutboundAliasResolver {
    maximum_alias_value : u16,

    current_aliases : HashMap<u16, String>,
}

impl ManualOutboundAliasResolver {
    pub fn new(maximum_alias_value : u16) -> ManualOutboundAliasResolver {
        ManualOutboundAliasResolver {
            maximum_alias_value,
            current_aliases : HashMap::new(),
        }
    }
}

impl OutboundAliasResolver for ManualOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16 { self.maximum_alias_value }

    fn reset_for_new_connection(&mut self, maximum_alias_value : u16) {
        self.maximum_alias_value = maximum_alias_value;
        self.current_aliases.clear();
    }

    fn resolve_topic_alias(&self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        if let Some(alias_ref_value) = alias {
            let alias_value = *alias_ref_value;
            if let Some(existing_alias) = self.current_aliases.get(alias_ref_value) {
                if *existing_alias == *topic {
                    return  OutboundAliasResolution {
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

pub struct LruOutboundAliasResolver {
    maximum_alias_value : u16,

    cache : LruCache<String, u16>
}

impl LruOutboundAliasResolver {
    pub fn new(maximum_alias_value : u16) -> LruOutboundAliasResolver {
        LruOutboundAliasResolver {
            maximum_alias_value,
            cache : LruCache::new(NonZeroUsize::new(maximum_alias_value as usize).unwrap())
        }
    }
}

impl OutboundAliasResolver for LruOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16 { self.maximum_alias_value }

    fn reset_for_new_connection(&mut self, maximum_alias_value : u16) {
        self.maximum_alias_value = maximum_alias_value;
        self.cache.clear();
    }

    fn resolve_topic_alias(&self, _: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        if self.maximum_alias_value == 0 {
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
        if alias_value > self.maximum_alias_value {
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

    fn resolve_and_apply_topic_alias(&mut self, alias: &Option<u16>, topic: &str) -> OutboundAliasResolution {
        let resolution = self.resolve_topic_alias(alias, topic);
        if resolution.alias.is_none() {
            return resolution;
        }

        if resolution.skip_topic {
            self.cache.promote(topic);
        } else {
            if self.cache.len() == self.maximum_alias_value as usize {
                self.cache.pop_lru();
            }

            let resolved_alias = resolution.alias.unwrap();
            self.cache.push(topic.to_string(), resolved_alias);
        }

        resolution
    }
}

pub struct InboundAliasResolver {
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
                return Err(MqttError::InboundTopicAliasNotValid);
            }

            if *alias_value == 0 || *alias_value > self.maximum_alias_value {
                error!("Topic Alias Resolution - inbound alias out of range");
                return Err(MqttError::InboundTopicAliasNotValid);
            }

            self.current_aliases.insert(*alias_value, topic.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::alias::*;
    use crate::MqttError;

    #[test]
    fn outbound_topic_alias_null() {
        let mut resolver = NullOutboundAliasResolver::new();

        assert_eq!(resolver.get_maximum_alias_value(), 0);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});

        resolver.reset_for_new_connection(10);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{skip_topic: false, alias: None});
    }

    #[test]
    fn outbound_topic_alias_manual_invalid() {
        let mut resolver = ManualOutboundAliasResolver::new(20);

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
        let mut resolver = ManualOutboundAliasResolver::new(20);

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
        let mut resolver = ManualOutboundAliasResolver::new(20);

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

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
    }

    #[test]
    fn outbound_topic_alias_lru_b_a_br() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_ar_br() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_and_apply_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{skip_topic: true, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_c_br_cr_br_cr_a() {
        let mut resolver = LruOutboundAliasResolver::new(2);

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

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut topic1), Ok(()));
        assert_eq!(topic1, "topic1");

        assert_eq!(resolver.resolve_topic_alias(&Some(10), &mut topic2), Ok(()));
        assert_eq!(topic2, "topic2");

        let mut unresolved_topic1 = "".to_string();
        let mut unresolved_topic2 = "".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut unresolved_topic1), Ok(()));
        assert_eq!(unresolved_topic1, "topic1");

        assert_eq!(resolver.resolve_topic_alias(&Some(10), &mut unresolved_topic2), Ok(()));
        assert_eq!(unresolved_topic2, "topic2");
    }

    #[test]
    fn inbound_topic_alias_resolve_success_rebind_existing() {
        let mut resolver = InboundAliasResolver::new(2);

        let mut topic1 = "topic1".to_string();
        let mut topic3 = "topic3".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut topic1), Ok(()));
        assert_eq!(topic1, "topic1");

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut topic3), Ok(()));
        assert_eq!(topic3, "topic3");

        let mut unresolved_topic3 = "".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut unresolved_topic3), Ok(()));
        assert_eq!(unresolved_topic3, "topic3");
    }


    #[test]
    fn inbound_topic_alias_resolve_failures() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(0), &mut topic1), Err(MqttError::InboundTopicAliasNotValid));
        assert_eq!(resolver.resolve_topic_alias(&Some(11), &mut topic1), Err(MqttError::InboundTopicAliasNotValid));

        let mut empty_topic = "".to_string();
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &mut empty_topic), Err(MqttError::InboundTopicAliasNotValid));
    }

    #[test]
    fn inbound_topic_alias_resolve_reset() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut topic1), Ok(()));

        let mut empty_topic = "".to_string();
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut empty_topic), Ok(()));
        assert_eq!(empty_topic, "topic1");

        resolver.reset_for_new_connection();
        empty_topic = "".to_string();
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut empty_topic), Err(MqttError::InboundTopicAliasNotValid));
    }
}

