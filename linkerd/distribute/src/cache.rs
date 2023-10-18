use super::params;
use linkerd_stack::{layer, ExtractParam, NewService};
use parking_lot::Mutex;
use std::{fmt::Debug, hash::Hash, sync::Arc};

/// A [`NewService`] that produces [`BackendCache`]s using a shared cache of
/// backends.
///
/// On each call to [`NewService::new_service`], the cache extracts a new set of
/// [`params::Backends`] from the target to determine which
/// services should be added/removed from the cache.
#[derive(Debug)]
pub struct NewBackendCache<K, X, N, S> {
    extract: X,
    inner: N,
    backends: Arc<Mutex<ahash::AHashMap<K, S>>>,
}

#[derive(Debug)]
pub struct BackendCache<K, S> {
    backends: Arc<ahash::AHashMap<K, S>>,
}

// === impl BackendCache ===

impl<K, X: Clone, N, S> NewBackendCache<K, X, N, S> {
    pub fn new(inner: N, extract: X) -> Self {
        Self {
            extract,
            inner,
            backends: Default::default(),
        }
    }

    pub fn layer_via(extract: X) -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(inner, extract.clone()))
    }
}

impl<K, N, S> NewBackendCache<K, (), N, S> {
    pub fn layer() -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(|inner| Self::new(inner, ()))
    }
}

impl<T, K, X, N, KNew, S> NewService<T> for NewBackendCache<K, X, N, S>
where
    X: ExtractParam<params::Backends<K>, T>,
    N: NewService<T, Service = KNew>,
    K: Eq + Hash + Clone + Debug,
    KNew: NewService<K, Service = S>,
    S: Clone,
{
    type Service = BackendCache<K, S>;

    fn new_service(&self, target: T) -> Self::Service {
        // 此处的 target 是 Params<Http<HttpSidecar>>
        // 提取出 Backends
        // 这里的 Backends 集合包含的是 Concrete 对象
        let params::Backends(backends) = self.extract.extract_param(&target);
        // Params<Http<HttpSidecar>> 传递到下游 
        let newk = self.inner.new_service(target);

        let mut cache = self.backends.lock();

        // Remove all backends that aren't in the updated set of addrs.
        // 判断缓存中的 backend 是否包含传递下来的 backend
        cache.retain(|backend, _| {
            if backends.contains(backend) {
                true
            } else {
                tracing::debug!(?backend, "Removing");
                false
            }
        });

        // If there are additional addrs, cache a new service for each.
        debug_assert!(backends.len() >= cache.len());
        // 判断传递下来的 backends 集合长度是否大于 cache 集合长度
        // 下面就是做更新缓存的操作
        // 缓存里存放的是 Concrete -> 具体的 service
        if backends.len() > cache.len() {
            cache.reserve(backends.len());
            for backend in &*backends {
                // Skip rebuilding targets we already have a stack for.
                if cache.contains_key(backend) {
                    tracing::trace!(?backend, "Retaining");
                    continue;
                }

                tracing::debug!(?backend, "Adding");
                cache.insert(backend.clone(), newk.new_service(backend.clone()));
            }
        }

        BackendCache {
            backends: Arc::new((*cache).clone()),
        }
    }
}

impl<K, X: Clone, N: Clone, S> Clone for NewBackendCache<K, X, N, S> {
    fn clone(&self) -> Self {
        Self {
            extract: self.extract.clone(),
            inner: self.inner.clone(),
            backends: self.backends.clone(),
        }
    }
}

// === impl BackendCache ===

impl<K, S> NewService<K> for BackendCache<K, S>
where
    K: Eq + Hash + Clone + Debug,
    S: Clone,
{
    type Service = S;

    fn new_service(&self, target: K) -> Self::Service {
        self.backends
            .get(&target)
            .expect("target must be in cache")
            .clone()
    }
}

impl<K, S> Clone for BackendCache<K, S> {
    fn clone(&self) -> Self {
        Self {
            backends: self.backends.clone(),
        }
    }
}
