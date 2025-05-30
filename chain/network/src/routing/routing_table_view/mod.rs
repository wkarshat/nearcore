use crate::routing;
use lru::LruCache;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(test)]
mod tests;

const LAST_ROUTED_CACHE_SIZE: usize = 10_000;

pub(crate) struct RoutingTableView(Mutex<Inner>);

struct Inner {
    /// For each peer, the set of neighbors which are one hop closer to `my_peer_id`.
    /// Alternatively, if we look at the set of all shortest path from `my_peer_id` to peer,
    /// this will be the set of first nodes on all such paths.
    next_hops: Arc<routing::NextHopTable>,

    /// Contains the shortest path length for each routable peer in the network.
    /// Used only to collect metrics measuring routing performance.
    /// TODO(saketh): Remove this when we deprecate the V1 routing protocol.
    distance: Arc<routing::DistanceTable>,

    /// Counter of number of calls to find_route_by_peer_id.
    find_route_calls: u64,
    /// Last time the given peer was selected by find_route_by_peer_id.
    last_routed: LruCache<PeerId, u64>,
}

impl Inner {
    /// Select a connected peer on some shortest path to `peer_id`.
    /// If there are several such peers, pick the least recently used one.
    fn find_next_hop(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        let peers = self.next_hops.get(peer_id).ok_or(FindRouteError::PeerUnreachable)?;
        let next_hop = peers
            .iter()
            .min_by_key(|p| self.last_routed.get(*p).copied().unwrap_or(0))
            .ok_or(FindRouteError::PeerUnreachable)?;
        self.last_routed.put(next_hop.clone(), self.find_route_calls);
        self.find_route_calls += 1;
        Ok(next_hop.clone())
    }

    fn update(
        &mut self,
        next_hops: Arc<routing::NextHopTable>,
        distance: Arc<routing::DistanceTable>,
    ) {
        self.next_hops = next_hops;
        self.distance = distance
    }
}

#[derive(Debug)]
pub(crate) enum FindRouteError {
    PeerUnreachable,
    RouteBackNotFound,
}

impl RoutingTableView {
    pub fn new() -> Self {
        Self(Mutex::new(Inner {
            next_hops: Default::default(),
            distance: Default::default(),
            find_route_calls: 0,
            last_routed: LruCache::new(NonZeroUsize::new(LAST_ROUTED_CACHE_SIZE).unwrap()),
        }))
    }

    pub(crate) fn update(
        &self,
        next_hops: Arc<routing::NextHopTable>,
        distance: Arc<routing::DistanceTable>,
    ) {
        self.0.lock().update(next_hops, distance)
    }

    pub(crate) fn reachable_peers(&self) -> usize {
        // There is an implicit assumption here that all next_hops entries are non-empty.
        // To enforce this, we would need to make NextHopTable a new type rather than an alias,
        // and add appropriate constructors, which would filter out empty entries.
        self.0.lock().next_hops.len()
    }

    // Given a PeerId to which we wish to route a message, returns the first hop on a
    // route to the target. If no route is known, produces FindRouteError.
    pub(crate) fn find_next_hop_for_target(
        &self,
        target: &PeerId,
    ) -> Result<PeerId, FindRouteError> {
        self.0.lock().find_next_hop(target)
    }

    pub(crate) fn get_distance(&self, peer_id: &PeerId) -> Option<u32> {
        self.0.lock().distance.get(peer_id).copied()
    }

    pub(crate) fn view_route(&self, peer_id: &PeerId) -> Option<Vec<PeerId>> {
        self.0.lock().next_hops.get(peer_id).cloned()
    }

    pub(crate) fn info(&self) -> RoutingTableInfo {
        let inner = self.0.lock();
        RoutingTableInfo { next_hops: inner.next_hops.clone() }
    }
}

#[derive(Debug)]
pub struct RoutingTableInfo {
    pub next_hops: Arc<routing::NextHopTable>,
}
