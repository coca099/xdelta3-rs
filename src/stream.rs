use futures_io::*;
use futures_util::io::*;
use std::{ffi::CStr, io, ops::Range};

use super::binding;
use log::debug;

#[allow(unused)]
const XD3_DEFAULT_WINSIZE: usize = 1 << 23;
const XD3_DEFAULT_SRCWINSZ: usize = 1 << 26;

/// Describes an I/O or xdelta3 failure while streaming a patch.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// Reading the primary input failed.
    #[error("failed to read input: {0}")]
    InputRead(#[source] io::Error),
    /// Reading the source file failed.
    #[error("failed to read source: {0}")]
    SourceRead(#[source] io::Error),
    /// Writing decoded or encoded output failed.
    #[error("failed to write output: {0}")]
    OutputWrite(#[source] io::Error),
    /// Flushing output failed.
    #[error("failed to flush output: {0}")]
    OutputFlush(#[source] io::Error),
    /// The source byte count exceeded the platform's addressable range.
    #[error("source size overflow")]
    SourceSizeOverflow,
    /// An xdelta3 operation failed.
    #[error(
        "xdelta3 error {code}: {message}",
        message = .message.as_deref().unwrap_or("no message")
    )]
    XDelta3 { code: i32, message: Option<String> },
}

fn xdelta_error(stream: &binding::xd3_stream, code: i32) -> StreamError {
    let message = if stream.msg.is_null() {
        None
    } else {
        Some(
            unsafe { CStr::from_ptr(stream.msg) }
                .to_string_lossy()
                .into_owned(),
        )
    };

    StreamError::XDelta3 { code, message }
}

async fn read_until_full_or_eof<R>(reader: &mut R, buffer: &mut [u8]) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut bytes_read = 0;
    while bytes_read < buffer.len() {
        let read_size = reader.read(&mut buffer[bytes_read..]).await?;
        if read_size == 0 {
            break;
        }
        bytes_read += read_size;
    }
    Ok(bytes_read)
}

struct SrcBuffer<R> {
    src: binding::xd3_source,
    read: R,
    read_len: usize,
    eof_known: bool,

    block_count: usize,
    block_offset: usize,
    buf: Box<[u8]>,
}

impl<R: AsyncRead + Unpin> SrcBuffer<R> {
    async fn new(mut read: R) -> std::result::Result<Self, StreamError> {
        let block_count = 64;
        let max_winsize = XD3_DEFAULT_SRCWINSZ;
        let blksize = max_winsize / block_count;

        let mut src: binding::xd3_source = unsafe { std::mem::zeroed() };
        src.blksize = blksize as u32;
        src.max_winsize = max_winsize as u64;

        let mut buf = Vec::with_capacity(max_winsize);
        buf.resize(max_winsize, 0u8);

        let read_len = read_until_full_or_eof(&mut read, &mut buf)
            .await
            .map_err(StreamError::SourceRead)?;
        debug!("SrcBuffer::new read_len={}", read_len);

        Ok(Self {
            src,
            read,
            read_len,
            eof_known: read_len != buf.len(),

            block_count,
            block_offset: 0,
            buf: buf.into_boxed_slice(),
        })
    }

    async fn fetch(&mut self) -> std::result::Result<bool, StreamError> {
        let idx = self.block_offset;
        let block_size = self.src.blksize as usize;
        let start = block_size * (idx % self.block_count);
        let r = start..start + block_size;
        let block = &mut self.buf[r.clone()];
        let block_len = block.len();
        let read_len = read_until_full_or_eof(&mut self.read, block)
            .await
            .map_err(StreamError::SourceRead)?;
        block[read_len..].fill(0);
        debug!(
            "range={:?}, block_len={}, read_len={}",
            r, block_len, read_len,
        );

        self.block_offset = self
            .block_offset
            .checked_add(1)
            .ok_or(StreamError::SourceSizeOverflow)?;
        self.read_len = self
            .read_len
            .checked_add(read_len)
            .ok_or(StreamError::SourceSizeOverflow)?;

        Ok(read_len != block_len)
    }

    async fn prepare(&mut self, idx: usize) -> std::result::Result<(), StreamError> {
        while !self.eof_known && idx >= self.block_offset + self.block_count {
            debug!(
                "prepare idx={}, block_offset={}, block_count={}",
                idx, self.block_offset, self.block_count
            );
            let eof = self.fetch().await?;
            if eof {
                debug!("eof");
                self.eof_known = true;
                break;
            }
        }
        Ok(())
    }

    fn block_range(&self, idx: usize) -> Range<usize> {
        debug!("idx={}, offset={}", idx, self.block_offset);
        assert!(idx >= self.block_offset && idx < self.block_offset + self.block_count);

        let block_index = idx;
        let ring_index = idx % self.block_count;
        let block_size = self.src.blksize as usize;
        let source_offset = block_index.saturating_mul(block_size);
        let block_len = self.read_len.saturating_sub(source_offset).min(block_size);
        let start = block_size * ring_index;

        start..start + block_len
    }

    async fn getblk(&mut self) -> std::result::Result<(), StreamError> {
        debug!(
            "getsrcblk: curblkno={}, getblkno={}",
            self.src.curblkno, self.src.getblkno,
        );

        let blkno = self.src.getblkno as usize;
        self.prepare(blkno).await?;
        let range = self.block_range(blkno);

        let src = &mut self.src;
        let data = &self.buf[range];

        src.curblkno = src.getblkno;
        src.curblk = data.as_ptr();
        src.onblk = data.len() as u32;

        src.eof_known = self.eof_known as i32;
        if !self.eof_known {
            src.max_blkno = src.curblkno;
            src.onlastblk = src.onblk;
        } else {
            let last_byte = self.read_len.saturating_sub(1);
            let block_size = src.blksize as usize;
            src.max_blkno = (last_byte / block_size) as u64;
            src.onlastblk = if self.read_len == 0 {
                0
            } else {
                (last_byte % block_size + 1) as u32
            };
        }

        Ok(())
    }
}

struct Xd3Stream {
    inner: binding::xd3_stream,
}
impl Xd3Stream {
    fn new() -> Self {
        let inner: binding::xd3_stream = unsafe { std::mem::zeroed() };
        return Self { inner };
    }
}
impl Drop for Xd3Stream {
    fn drop(&mut self) {
        unsafe {
            binding::xd3_free_stream(&mut self.inner as *mut _);
        }
    }
}

/// Decodes a VCDIFF input, returning `None` on failure without the error details.
///
/// Use [`try_decode_async`] to receive the specific I/O or xdelta3 error.
#[deprecated(note = "use try_decode_async to preserve error details")]
pub async fn decode_async<R1, R2, W>(input: R1, src: R2, out: W) -> Option<()>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    try_decode_async(input, src, out).await.ok()
}

/// Decodes a VCDIFF input against a source and streams the result to `out`.
///
/// # Errors
/// Returns [`StreamError`] if reading either input, writing or flushing output,
/// or an xdelta3 operation fails.
pub async fn try_decode_async<R1, R2, W>(
    input: R1,
    src: R2,
    out: W,
) -> std::result::Result<(), StreamError>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    process_async(Mode::Decode, input, src, out).await
}

/// Encodes `input` against `src`, returning `None` on failure without the error details.
///
/// Use [`try_encode_async`] to receive the specific I/O or xdelta3 error.
#[deprecated(note = "use try_encode_async to preserve error details")]
pub async fn encode_async<R1, R2, W>(input: R1, src: R2, out: W) -> Option<()>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    try_encode_async(input, src, out).await.ok()
}

/// Encodes `input` against `src` and streams the VCDIFF patch to `out`.
///
/// # Errors
/// Returns [`StreamError`] if reading either input, writing or flushing output,
/// or an xdelta3 operation fails.
pub async fn try_encode_async<R1, R2, W>(
    input: R1,
    src: R2,
    out: W,
) -> std::result::Result<(), StreamError>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    process_async(Mode::Encode, input, src, out).await
}

enum Mode {
    Encode,
    Decode,
}

async fn process_async<R1, R2, W>(
    mode: Mode,
    mut input: R1,
    src: R2,
    mut out: W,
) -> std::result::Result<(), StreamError>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut stream = Xd3Stream::new();
    let stream = &mut stream.inner;
    let mut cfg: binding::xd3_config = unsafe { std::mem::zeroed() };
    cfg.winsize = XD3_DEFAULT_WINSIZE as u32;

    let mut src_buf = SrcBuffer::new(src).await?;

    let ret = unsafe { binding::xd3_config_stream(stream, &mut cfg) };
    if ret != 0 {
        return Err(xdelta_error(stream, ret));
    }

    let ret = unsafe { binding::xd3_set_source(stream, &mut src_buf.src) };
    if ret != 0 {
        return Err(xdelta_error(stream, ret));
    }

    let input_buf_size = stream.winsize as usize;
    debug!("stream.winsize={}", input_buf_size);
    let mut input_buf = Vec::with_capacity(input_buf_size);
    input_buf.resize(input_buf_size, 0u8);
    let mut eof = false;

    'outer: while !eof {
        let read_size = input
            .read(&mut input_buf)
            .await
            .map_err(StreamError::InputRead)?;
        debug!("read_size={}", read_size);
        if read_size == 0 {
            // xd3_set_flags
            stream.flags = binding::xd3_flags::XD3_FLUSH as i32;
            eof = true;
        }

        // xd3_avail_input
        stream.next_in = input_buf.as_ptr();
        stream.avail_in = read_size as u32;

        loop {
            let ret: binding::xd3_rvalues = unsafe {
                std::mem::transmute(match mode {
                    Mode::Encode => binding::xd3_encode_input(stream),
                    Mode::Decode => binding::xd3_decode_input(stream),
                })
            };

            if stream.msg != std::ptr::null() {
                debug!("ret={:?}, msg={:?}", ret, unsafe {
                    std::ffi::CStr::from_ptr(stream.msg)
                },);
            } else {
                debug!("ret={:?}", ret,);
            }

            use binding::xd3_rvalues::*;
            match ret {
                XD3_INPUT => {
                    continue 'outer;
                    //
                }
                XD3_OUTPUT => {
                    let mut out_data = unsafe {
                        std::slice::from_raw_parts(stream.next_out, stream.avail_out as usize)
                    };
                    while !out_data.is_empty() {
                        let n = out
                            .write(out_data)
                            .await
                            .map_err(StreamError::OutputWrite)?;
                        if n == 0 {
                            return Err(StreamError::OutputWrite(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "output writer made no progress",
                            )));
                        }
                        out_data = &out_data[n..];
                    }

                    // xd3_consume_output
                    stream.avail_out = 0;
                }
                XD3_GETSRCBLK => {
                    src_buf.getblk().await?;
                }
                XD3_GOTHEADER | XD3_WINSTART | XD3_WINFINISH => {
                    // do nothing
                }
                XD3_TOOFARBACK | XD3_INTERNAL | XD3_INVALID | XD3_INVALID_INPUT | XD3_NOSECOND
                | XD3_UNIMPLEMENTED => {
                    return Err(xdelta_error(stream, ret as i32));
                }
            }
        }
    }

    if let Mode::Decode = mode {
        let ret = unsafe { binding::xd3_close_stream(stream) };
        if ret != 0 {
            return Err(xdelta_error(stream, ret));
        }
    }

    out.flush().await.map_err(StreamError::OutputFlush)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    struct ShortReader<'a> {
        bytes: &'a [u8],
        max_read: usize,
    }

    impl AsyncRead for ShortReader<'_> {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let reader = self.get_mut();
            let read_size = buffer.len().min(reader.max_read).min(reader.bytes.len());
            buffer[..read_size].copy_from_slice(&reader.bytes[..read_size]);
            reader.bytes = &reader.bytes[read_size..];
            Poll::Ready(Ok(read_size))
        }
    }

    #[test]
    fn source_buffer_fills_initial_window_across_short_reads() {
        let source = vec![0x5a; 1024 * 1024];
        let reader = ShortReader {
            bytes: &source,
            max_read: 4096,
        };

        let source_buffer = futures::executor::block_on(SrcBuffer::new(reader))
            .expect("source reads should succeed");

        assert!(source_buffer.eof_known);
        assert_eq!(source_buffer.read_len, source.len());
        assert_eq!(source_buffer.block_range(0).len(), source.len());
        assert_eq!(source_buffer.block_range(1).len(), 0);
    }

    #[test]
    fn source_buffer_fills_wrapped_block_and_tracks_partial_eof() {
        const BLOCK_SIZE: usize = 1024 * 1024;
        const PARTIAL_BLOCK_SIZE: usize = BLOCK_SIZE / 2;

        let source = vec![0x5a; XD3_DEFAULT_SRCWINSZ + PARTIAL_BLOCK_SIZE];
        let reader = ShortReader {
            bytes: &source,
            max_read: 4096,
        };
        let mut source_buffer = futures::executor::block_on(SrcBuffer::new(reader))
            .expect("initial source reads should succeed");

        assert!(!source_buffer.eof_known);
        assert_eq!(source_buffer.read_len, XD3_DEFAULT_SRCWINSZ);

        source_buffer.src.getblkno = source_buffer.block_count as u64;
        futures::executor::block_on(source_buffer.getblk())
            .expect("wrapped source reads should succeed");

        assert!(source_buffer.eof_known);
        assert_eq!(source_buffer.read_len, source.len());
        assert_eq!(source_buffer.block_range(64).len(), PARTIAL_BLOCK_SIZE);
        assert_eq!(source_buffer.src.onblk, PARTIAL_BLOCK_SIZE as u32);
        assert_eq!(source_buffer.src.max_blkno, 64);
        assert_eq!(source_buffer.src.onlastblk, PARTIAL_BLOCK_SIZE as u32);
        assert!(source_buffer.buf[PARTIAL_BLOCK_SIZE..BLOCK_SIZE]
            .iter()
            .all(|byte| *byte == 0));
    }
}
