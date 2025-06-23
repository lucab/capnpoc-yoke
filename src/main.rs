//! A Rust example showing zero-copy deserialization using Cap'n Proto and Yoke:
//!  - https://docs.rs/capnp
//!  - https://docs.rs/yoke

#[path = "generated/poc_capnp.rs"]
mod poc_capnp;

use bytes::{BufMut, Bytes, BytesMut};
use capnp::message::{Reader, ReaderOptions, TypedBuilder, TypedReader};
use capnp::serialize::{self, BufferSegments};
use std::sync::mpsc;
use std::thread;
use yoke::{Yoke, Yokeable};

use crate::poc_capnp::http::header as http_header;
use crate::poc_capnp::http::header::Owned as OwnedHttpHeader;

/// Typed reader with owned buffer for a HTTP Header.
type HttpHeaderReader = TypedReader<BufferSegments<Bytes>, OwnedHttpHeader>;

/// A view into a valid HTTP Header.
#[derive(Yokeable, Debug)]
struct HttpHeaderView<'a> {
    key: &'a str,
    value: &'a str,
}

/// A deserialized HTTP Header message.
///
/// This owns the underlying buffer and offers an ergonomic view into the
/// validated data it contains.
struct HttpHeader {
    parsed: Yoke<HttpHeaderView<'static>, Box<HttpHeaderReader>>,
}

impl HttpHeader {
    pub fn serialize(key: String, val: String) -> anyhow::Result<Bytes> {
        let mut message = TypedBuilder::<OwnedHttpHeader>::new_default();
        let mut header_builder = message.init_root();
        header_builder.set_key(key);
        header_builder.set_value(val);

        let mut bufwr = {
            let size = serialize::compute_serialized_size_in_words(message.borrow_inner()) * 8;
            let buf = BytesMut::with_capacity(size);
            buf.writer()
        };
        serialize::write_message(&mut bufwr, message.borrow_inner())?;
        let buf = bufwr.into_inner().freeze();
        Ok(buf)
    }

    pub fn deserialize(data: Bytes) -> anyhow::Result<Self> {
        let buf = BufferSegments::new(data, ReaderOptions::default())?;
        let reader = Reader::new(buf, ReaderOptions::default()).into_typed();
        let cart = Box::new(reader);
        let parsed = Yoke::try_attach_to_cart(cart, |data| -> anyhow::Result<_> {
            let message: http_header::Reader = data.get()?;
            let key = message.get_key()?.to_str()?;
            let value = message.get_value()?.to_str()?;
            let view = HttpHeaderView { key, value };
            Ok(view)
        })?;
        Ok(Self { parsed })
    }

    pub fn view(&self) -> &HttpHeaderView {
        self.parsed.get()
    }
}

pub fn main() -> anyhow::Result<()> {
    let (header_tx, header_rx) = mpsc::channel();

    thread::spawn(move || {
        let http_header_data =
            HttpHeader::serialize("foo_key".repeat(7000), "bar_value".repeat(9000)).unwrap();
        let http_header = HttpHeader::deserialize(http_header_data).unwrap();
        header_tx.send(http_header).unwrap();
    });

    let http_header = header_rx.recv()?;

    let view = http_header.view();
    println!("'{}': '{}'", view.key, view.value);

    Ok(())
}
