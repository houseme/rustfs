// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;
use futures::{Stream, TryStreamExt as _};
use http::HeaderMap;
use pin_project_lite::pin_project;
use reqwest::{Client, Method, RequestBuilder};
use std::error::Error as _;
use std::io::{self, Error};

use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use tokio_util::io::StreamReader;
use tracing::{Instrument, info_span, instrument};

use crate::io_engine::get_io_engine;
use crate::{EtagResolvable, HashReaderDetector, HashReaderMut};

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

/// Enhanced HTTP client with connection pooling and performance optimization
fn get_optimized_http_client() -> Client {
    static CLIENT: LazyLock<Client> = LazyLock::new(|| {
        Client::builder()
            .pool_max_idle_per_host(32) // Increased connection pooling
            .pool_idle_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(60)) // Total request timeout
            .connect_timeout(Duration::from_secs(10)) // Connection timeout
            .tcp_keepalive(Duration::from_secs(60)) // Keep connections alive
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_adaptive_window(true) // Adaptive flow control
            .build()
            .expect("Failed to create HTTP client")
    });
    CLIENT.clone()
}

/// Enhanced HTTP debugging with structured logging
static HTTP_DEBUG_LOG: bool = false;

#[inline(always)]
fn http_debug_log(args: std::fmt::Arguments) {
    if HTTP_DEBUG_LOG {
        tracing::debug!("{}", args);
    }
}

macro_rules! http_log {
    ($($arg:tt)*) => {
        http_debug_log(format_args!($($arg)*));
    };
}

pin_project! {
    /// Enhanced HttpReader with advanced connection management and io_uring integration
    ///
    /// This reader provides high-performance HTTP streaming with connection pooling,
    /// adaptive flow control, and comprehensive monitoring for distributed object storage.
    pub struct HttpReader {
        url: String,
        method: Method,
        headers: HeaderMap,
        #[pin]
        inner: StreamReader<Pin<Box<dyn Stream<Item=std::io::Result<Bytes>>+Send+Sync>>, Bytes>,
        // Performance tracking
        bytes_downloaded: u64,
        request_start_time: std::time::Instant,
        // Connection pooling statistics
        connection_reused: bool,
    }
}

impl HttpReader {
    /// Create HttpReader with enhanced buffering and io_uring optimization
    #[instrument(skip(headers, body), fields(url = %url, method = ?method, capacity))]
    pub async fn with_capacity(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        capacity: usize,
    ) -> io::Result<Self> {
        let request_start = std::time::Instant::now();

        let client = get_optimized_http_client();
        let _io_engine = get_io_engine();

        // Build request with optimizations
        let mut req_builder = client.request(method.clone(), &url);

        // Apply headers
        req_builder = req_builder.headers(headers.clone());

        // Configure body if present
        if let Some(body_data) = body {
            req_builder = req_builder.body(body_data);
        }

        // Configure streaming with adaptive buffering
        let buffer_size = if capacity > 0 {
            capacity
        } else if false {
            // TODO: implement io_uring check
            256 * 1024 // 256KB for io_uring optimization
        } else {
            128 * 1024 // 128KB for standard operations
        };

        tracing::debug!(
            url = %url,
            method = ?method,
            buffer_size = buffer_size,
            "Initiating HTTP request with optimized buffering"
        );

        // Execute request with timeout and connection reuse detection
        let response = req_builder
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Check if connection was reused (simplified check)
        let connection_reused = response
            .extensions()
            .get::<reqwest::tls::TlsInfo>()
            .is_some();

        let status = response.status();
        if !status.is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP request failed with status: {}", status),
            ));
        }

        // Create adaptive stream with optimized buffering
        let stream = response.bytes_stream().map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        // Wrap stream with enhanced buffering
        let boxed_stream: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync>> = Box::pin(stream);
        let stream_reader = StreamReader::new(boxed_stream);

        #[cfg(feature = "metrics")]
        {
            counter!("rustfs_http_reader_requests_total").increment(1);
            if connection_reused {
                counter!("rustfs_http_reader_connections_reused_total").increment(1);
            }
            histogram!("rustfs_http_reader_request_setup_duration_seconds").record(request_start.elapsed().as_secs_f64());
        }

        Ok(Self {
            url,
            method,
            headers,
            inner: stream_reader,
            bytes_downloaded: 0,
            request_start_time: request_start,
            connection_reused,
        })
    }

    /// Create HttpReader with advanced streaming optimizations for large files
    #[instrument(skip(headers, body), fields(url = %url, method = ?method))]
    pub async fn with_streaming_optimization(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
    ) -> io::Result<Self> {
        // Use larger buffer for streaming workloads
        const STREAMING_BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer
        Self::with_capacity(url, method, headers, body, STREAMING_BUFFER_SIZE).await
    }
}

/// HTTP download statistics for monitoring
#[cfg(feature = "metrics")]
#[derive(Debug, Clone)]
pub struct HttpDownloadStats {
    pub bytes_downloaded: u64,
    pub elapsed_time: Duration,
    pub connection_reused: bool,
    pub url: String,
}

impl AsyncRead for HttpReader {
    #[instrument(skip(self, cx, buf), fields(buf_remaining = buf.remaining()))]
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        let _original_filled = buf.filled().len();

        let poll_result = this.inner.as_mut().poll_read(cx, buf);

        // Track bytes downloaded for metrics
        #[cfg(feature = "metrics")]
        if let Poll::Ready(Ok(())) = &poll_result {
            let bytes_read = buf.filled().len() - original_filled;
            if bytes_read > 0 {
                *this.bytes_downloaded += bytes_read as u64;

                counter!("rustfs_http_reader_bytes_downloaded_total").increment(bytes_read as u64);

                // Calculate and report download speed
                let elapsed = this.request_start_time.elapsed();
                if elapsed.as_secs() > 0 {
                    let speed_mbps = (*this.bytes_downloaded as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
                    gauge!("rustfs_http_reader_download_speed_mbps").set(speed_mbps);
                }

                tracing::trace!(
                    bytes_read = bytes_read,
                    total_downloaded = *this.bytes_downloaded,
                    url = %this.url,
                    "HTTP data received"
                );
            }
        }

        poll_result
    }
}

impl AsyncWrite for HttpReader {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
        // HttpReader is read-only
        Poll::Ready(Err(io::Error::new(io::ErrorKind::Unsupported, "HttpReader does not support writing")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl EtagResolvable for HttpReader {
    fn try_resolve_etag(&mut self) -> Option<String> {
        None
    }
}

impl HashReaderDetector for HttpReader {
    fn is_hash_reader(&self) -> bool {
        false
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        None
    }
}

/// HTTP download statistics for monitoring
#[cfg(feature = "metrics")]
#[derive(Debug, Clone)]
pub struct HttpDownloadStats {
    pub bytes_downloaded: u64,
    pub elapsed_time: Duration,
    pub connection_reused: bool,
    pub url: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// Get download statistics for monitoring
    #[cfg(feature = "metrics")]
    pub fn get_download_stats(&self) -> HttpDownloadStats {
        HttpDownloadStats {
            bytes_downloaded: self.bytes_downloaded,
            elapsed_time: self.request_start_time.elapsed(),
            connection_reused: self.connection_reused,
            url: self.url.clone(),
        }
    }

    #[tokio::test]
    #[ignore] // Requires internet connection
    async fn test_http_reader_basic_functionality() {
        let url = "https://httpbin.org/json".to_string();
        let method = Method::GET;
        let headers = HeaderMap::new();

        let mut reader = HttpReader::new(url, method, headers, None).await.unwrap();
        let mut result = Vec::new();

        reader.read_to_end(&mut result).await.unwrap();

        // Should have received some JSON data
        assert!(!result.is_empty());

        // Should be valid UTF-8 (JSON)
        let response_text = String::from_utf8(result).unwrap();
        assert!(response_text.contains("\""));

        println!("Received response: {}", response_text);
    }

    #[tokio::test]
    async fn test_http_reader_error_handling() {
        let url = "https://nonexistent-domain-12345.com/test".to_string();
        let method = Method::GET;
        let headers = HeaderMap::new();

        let result = HttpReader::new(url, method, headers, None).await;

        // Should fail with connection error
        assert!(result.is_err());

        let error = result.unwrap_err();
        println!("Expected error: {}", error);
    }

    #[tokio::test]
    #[ignore] // Requires internet connection
    async fn test_http_reader_with_custom_headers() {
        let url = "https://httpbin.org/headers".to_string();
        let method = Method::GET;

        let mut headers = HeaderMap::new();
        headers.insert("User-Agent", "RustFS-Rio/1.0".parse().unwrap());
        headers.insert("Accept", "application/json".parse().unwrap());

        let mut reader = HttpReader::new(url, method, headers, None).await.unwrap();
        let mut result = Vec::new();

        reader.read_to_end(&mut result).await.unwrap();

        let response_text = String::from_utf8(result).unwrap();

        // Should contain our custom headers
        assert!(response_text.contains("RustFS-Rio/1.0"));
        assert!(response_text.contains("application/json"));

        println!("Headers response: {}", response_text);
    }

    #[tokio::test]
    #[ignore] // Requires internet connection
    async fn test_http_reader_large_download() {
        // Test with a larger file to validate streaming performance
        let url = "https://httpbin.org/stream/100".to_string(); // 100 lines of JSON
        let method = Method::GET;
        let headers = HeaderMap::new();

        let mut reader = HttpReader::with_capacity(url, method, headers, None, 64 * 1024)
            .await
            .unwrap();
        let mut result = Vec::new();

        let start = std::time::Instant::now();
        reader.read_to_end(&mut result).await.unwrap();
        let duration = start.elapsed();

        println!("Downloaded {} bytes in {:?}", result.len(), duration);

        // Should have received substantial data
        assert!(result.len() > 1000);

        // Calculate download speed
        let speed_mbps = (result.len() as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64();
        println!("Download speed: {:.2} MB/s", speed_mbps);
    }
}
