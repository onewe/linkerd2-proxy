//! A specialized `NewIdleCache` to manage discovery stae, usually from a
//! control plane client.

use futures::TryFutureExt;
use linkerd_error::Error;
use linkerd_idle_cache::{Cached, NewIdleCached};
use linkerd_stack::{
    layer, queue, CloneParam, FutureService, MapErrBoxed, NewQueueWithoutTimeout, NewService,
    Oneshot, Param, QueueWithoutTimeout, Service, ServiceExt, ThunkClone,
};
use std::{fmt, hash::Hash, task, time};

/// A [`NewService`] that extracts a `K`-typed key from each target to build a
/// [`Cached`]<[`DiscoverThunk`]>.
#[derive(Clone)]
pub struct NewCachedDiscover<K, D, N>
where
    K: Clone + fmt::Debug + Eq + Hash + Send + Sync + 'static,
    D: Service<K, Error = Error> + Clone + Send + Sync + 'static,
    D::Response: Clone + Send + Sync,
    D::Future: Send + Unpin,
{
    // NewService<K, Service<(), D::Response>>
    cache: NewIdleCached<K, NewQueueThunk<NewDiscoverThunk<D>>>,

    // NewService<D::Response>
    inner: N,
}

/// The future that drives discovery to build an new inner service wrapped
/// in the [`Cached`] decorator from the discovery lookup, preventing the
/// cache's idle timeout from starting until returned services are dropped.
#[derive(Debug)]
#[pin_project::pin_project]
pub struct CachedDiscoverFuture<D: Service<()>, N> {
    // NewService<D::Response>
    inner: N,

    // Holds a cache handle so that we can carry that forward with the returned
    // service.
    cached: Cached<D>,

    // A future that obtains a `D::Response` and produces an `N::Service`.
    #[pin]
    future: Oneshot<Cached<D>, ()>,
}

/// A [`Service<()>`] that uses a `D`-typed discovery service to build a new
/// inner service wrapped in the discovery service's cache handle.
pub type CachedDiscover<D, N, S> = FutureService<CachedDiscoverFuture<QueueThunk<D>, N>, Cached<S>>;

/// A [`Service<()>`] that discovers a `Rsp` and returns a clone of it for each
/// call.
pub type DiscoverThunk<Req, Rsp, S> = FutureService<
    futures::future::MapOk<Oneshot<S, Req>, fn(Rsp) -> MapErrBoxed<ThunkClone<Rsp>>>,
    MapErrBoxed<ThunkClone<Rsp>>,
>;

#[derive(Clone, Debug)]
struct NewDiscoverThunk<D> {
    discover: D,
}

// We do not enforce any timeouts on discovery. Nor are we concerned with load
// shedding. `NewCachedDiscover` returns a `FutureService`, so the internal
// queue's capacity can exert backpressure into `Service::poll_ready`. This is
// a good thing. That service stack can determine its own load shedding and
// failfast semantics independently. The queue capacity is purely to avoid
// contention across clones.
type NewQueueThunk<D> = NewQueueWithoutTimeout<CloneParam<queue::Capacity>, (), D>;
type QueueThunk<D> = QueueWithoutTimeout<(), D>;
const QUEUE_CAPACITY: queue::Capacity = queue::Capacity(10);

// === impl NewCachedDiscover ===

impl<K, D, N> NewCachedDiscover<K, D, N>
where
    K: Clone + fmt::Debug + Eq + Hash + Send + Sync + 'static,
    D: Service<K, Error = Error> + Clone + Send + Sync + 'static,
    D::Response: Clone + Send + Sync,
    D::Future: Send + Unpin,
{
    pub fn new(inner: N, discover: D, timeout: time::Duration) -> Self {
        // 利用上层传递过来的 discover 创建一个 queue, 默认容量为 10
        let queue = NewQueueThunk::new(
            NewDiscoverThunk { discover },
            CloneParam::from(QUEUE_CAPACITY),
        );
        // 这里的 cache 用于缓存 OrigDstAddr 对应的  profile 和 policy 
        Self {
            inner,
            cache: NewIdleCached::new(queue, timeout),
        }
    }

    pub fn layer(disco: D, idle: time::Duration) -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(inner, disco.clone(), idle))
    }
}

impl<T, K, D, M, N> NewService<T> for NewCachedDiscover<K, D, M>
where
    T: Param<K> + Clone,
    K: Clone + fmt::Debug + Eq + Hash + Send + Sync + 'static,
    D: Service<K, Error = Error> + Clone + Send + Sync + 'static,
    D::Response: Clone + Send + Sync + 'static,
    D::Future: Send + Unpin,
    M: NewService<T, Service = N> + Clone,
    N: NewService<D::Response> + Clone + Send + 'static,
{
    type Service = CachedDiscover<D::Response, N, N::Service>;

    fn new_service(&self, target: T) -> Self::Service {
        // 这里的 target 是 Accept , param 是 OrigDstAddr
        let key = target.param();
        // 使用 OrigDstAddr 创建一个 cache, OrigDstAddr 是数据包来源地址
        // 这里的 cache 用于缓存 OrigDstAddr 对应的  profile 和 policy 
        let cached = self.cache.new_service(key);
        let inner = self.inner.new_service(target);
        let future = cached.clone().oneshot(());
        FutureService::new(CachedDiscoverFuture {
            future,
            cached,
            inner,
        })
    }
}

// === impl NewDiscoverThunk ===

impl<T, D> NewService<T> for NewDiscoverThunk<D>
where
    D: Service<T, Error = Error> + Clone,
    D::Response: Clone,
    D::Future: Unpin,
{
    type Service = DiscoverThunk<T, D::Response, D>;

    fn new_service(&self, target: T) -> Self::Service {
        let disco = self.discover.clone().oneshot(target);
        FutureService::new(disco.map_ok(|rsp| ThunkClone::new(rsp).into()))
    }
}

// === impl DiscoverFUture ===

impl<D, N> std::future::Future for CachedDiscoverFuture<D, N>
where
    D: Service<()>,
    N: NewService<D::Response>,
{
    type Output = Result<Cached<N::Service>, D::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Self::Output> {
        let this = self.project();
        let discovery = futures::ready!(this.future.poll(cx))?;
        let inner = this.inner.new_service(discovery);
        // 如果创建 inner 的 service 成功则把 cache OrigDstAddr 对应的 (profile, policy) 替换成
        // inner 中的 Discovery<Accept> 对象 
        let cached = this.cached.clone_with(inner);
        task::Poll::Ready(Ok(cached))
    }
}
