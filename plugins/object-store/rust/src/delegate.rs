/// Implement [`crate::ObjectStore`] for a newtype over `Core<B>`.
macro_rules! delegate_object_store {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl $crate::ObjectStore for $ty {
            fn backend(&self) -> &'static str {
                self.core.name()
            }

            async fn put(
                &self,
                key: &$crate::ObjectKey,
                body: bytes::Bytes,
                opts: $crate::PutOpts,
            ) -> $crate::Result<$crate::PutAck> {
                self.core.put(key, body, opts).await
            }

            async fn head(
                &self,
                key: &$crate::ObjectKey,
            ) -> $crate::Result<Option<$crate::ObjectMeta>> {
                self.core.head(key).await
            }

            async fn get(
                &self,
                key: &$crate::ObjectKey,
                range: Option<std::ops::Range<u64>>,
            ) -> $crate::Result<$crate::ByteStream> {
                self.core.get(key, range).await
            }

            async fn list(
                &self,
                prefix: &$crate::ObjectKey,
                cursor: Option<String>,
            ) -> $crate::Result<$crate::Page<$crate::ObjectMeta>> {
                self.core.list(prefix, cursor).await
            }

            async fn presign_get(
                &self,
                key: &$crate::ObjectKey,
                ttl: std::time::Duration,
            ) -> $crate::Result<url::Url> {
                self.core.presign_get(key, ttl).await
            }

            async fn multipart(
                &self,
                key: &$crate::ObjectKey,
                opts: $crate::MultipartOpts,
            ) -> $crate::Result<Box<dyn $crate::MultipartUpload>> {
                self.core.multipart(key, opts).await
            }
        }
    };
}
pub(crate) use delegate_object_store;
