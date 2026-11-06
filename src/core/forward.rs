//! Test-only macro for `ObjectStore` doubles that forward most calls to an
//! inner store.

/// Implements `ObjectStore` for `$ty`: the methods named in `forward` pass
/// straight to the store in field `$inner`, and the `{ ... }` block holds the
/// methods the double implements itself (all seven required methods must be in
/// one place or the other). Written as one macro that builds the whole impl,
/// because `#[async_trait]` only rewrites the methods it sees when it runs.
///
/// ```ignore
/// forward_object_store! {
///     impl ObjectStore for Spy => inner;
///     forward [put_opts, list];
///     { async fn get_opts(&self, ...) -> ... { ... } }
/// }
/// ```
macro_rules! forward_object_store {
    (
        impl ObjectStore for $ty:ty => $inner:ident;
        forward [$($method:ident),* $(,)?];
        { $($own:tt)* }
    ) => {
        forward_object_store!(@build $ty, $inner, [$($method)*], [$($own)*]);
    };
    (@build $ty:ty, $inner:ident, [], [$($acc:tt)*]) => {
        #[async_trait::async_trait]
        impl object_store::ObjectStore for $ty {
            $($acc)*
        }
    };
    (@build $ty:ty, $inner:ident, [put_opts $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            async fn put_opts(
                &self,
                location: &object_store::path::Path,
                payload: object_store::PutPayload,
                opts: object_store::PutOptions,
            ) -> object_store::Result<object_store::PutResult> {
                self.$inner.put_opts(location, payload, opts).await
            }
        ]);
    };
    (@build $ty:ty, $inner:ident, [put_multipart_opts $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            async fn put_multipart_opts(
                &self,
                location: &object_store::path::Path,
                opts: object_store::PutMultipartOptions,
            ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
                self.$inner.put_multipart_opts(location, opts).await
            }
        ]);
    };
    (@build $ty:ty, $inner:ident, [get_opts $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            async fn get_opts(
                &self,
                location: &object_store::path::Path,
                options: object_store::GetOptions,
            ) -> object_store::Result<object_store::GetResult> {
                self.$inner.get_opts(location, options).await
            }
        ]);
    };
    (@build $ty:ty, $inner:ident, [delete_stream $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            fn delete_stream(
                &self,
                locations: futures::stream::BoxStream<
                    'static,
                    object_store::Result<object_store::path::Path>,
                >,
            ) -> futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            > {
                self.$inner.delete_stream(locations)
            }
        ]);
    };
    (@build $ty:ty, $inner:ident, [list $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            fn list(
                &self,
                prefix: Option<&object_store::path::Path>,
            ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
            {
                self.$inner.list(prefix)
            }
        ]);
    };
    (@build $ty:ty, $inner:ident, [list_with_delimiter $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            async fn list_with_delimiter(
                &self,
                prefix: Option<&object_store::path::Path>,
            ) -> object_store::Result<object_store::ListResult> {
                self.$inner.list_with_delimiter(prefix).await
            }
        ]);
    };
    (@build $ty:ty, $inner:ident, [copy_opts $($rest:ident)*], [$($acc:tt)*]) => {
        forward_object_store!(@build $ty, $inner, [$($rest)*], [$($acc)*
            async fn copy_opts(
                &self,
                from: &object_store::path::Path,
                to: &object_store::path::Path,
                options: object_store::CopyOptions,
            ) -> object_store::Result<()> {
                self.$inner.copy_opts(from, to, options).await
            }
        ]);
    };
}
