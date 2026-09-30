#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    #[cfg(feature = "stream")]
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    #[cfg(feature = "stream")]
    use xdelta3::stream::*;
    use xdelta3::*;

    #[cfg(feature = "stream")]
    struct FailingReader;

    #[cfg(feature = "stream")]
    impl futures::io::AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::other("reader failed")))
        }
    }

    #[cfg(feature = "stream")]
    struct TestWriter {
        fail_write: bool,
        zero_write: bool,
        fail_flush: bool,
    }

    #[cfg(feature = "stream")]
    impl futures::io::AsyncWrite for TestWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let writer = self.get_mut();
            if writer.fail_write {
                Poll::Ready(Err(io::Error::other("writer failed")))
            } else if writer.zero_write {
                Poll::Ready(Ok(0))
            } else {
                Poll::Ready(Ok(buffer.len()))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.get_mut().fail_flush {
                Poll::Ready(Err(io::Error::other("flush failed")))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[cfg(feature = "stream")]
    fn encode2(input: &[u8], src: &[u8]) -> Result<Vec<u8>, StreamError> {
        let mut out = Vec::new();
        futures::executor::block_on(try_encode_async(input, src, &mut out))?;
        Ok(out)
    }

    #[cfg(feature = "stream")]
    fn decode2(input: &[u8], src: &[u8]) -> Result<Vec<u8>, StreamError> {
        let mut out = Vec::new();
        futures::executor::block_on(try_decode_async(input, src, &mut out))?;
        Ok(out)
    }

    fn check_decode(input: &[u8], src: &[u8]) -> Vec<u8> {
        let out_mem = decode(input, src).expect("Failed to decode");
        #[cfg(feature = "stream")]
        {
            let out_async = decode2(input, src).expect("Failed to decode");
            assert_eq!(out_mem, out_async);
        }
        out_mem
    }

    #[test]
    fn basic_recoding() {
        let result =
            encode(&[1, 2, 3, 4, 5, 6, 7], &[1, 2, 4, 4, 7, 6, 7]).expect("failed to encode");
        let recode = check_decode(&result, &[1, 2, 4, 4, 7, 6, 7]);
        assert_eq!(&recode, &[1, 2, 3, 4, 5, 6, 7]);
    }

    fn read_file(filename: &str) -> Vec<u8> {
        let mut file = File::open(filename).expect("Failed to open file");
        let mut data = Vec::new();

        file.read_to_end(&mut data).expect("Failed to read file");

        data
    }

    #[test]
    fn xdelta_own_test() {
        let fixure_path = "xdelta3/xdelta3/examples/iOS/xdelta3-ios-test/xdelta3-ios-test/";
        let original_data = read_file(&format!("{}/{}", fixure_path, "file_v1.bin"));
        let correct_data = read_file(&format!("{}/{}", fixure_path, "file_v2.bin"));
        let patch_data = read_file(&format!("{}/{}", fixure_path, "file_v1_to_v2.bin"));

        let patched_data = check_decode(&patch_data, &original_data);
        assert_eq!(patched_data, correct_data);
    }

    #[test]
    #[cfg(feature = "stream")]
    fn round_trip_test() {
        let fixure_path = "xdelta3/xdelta3/examples/iOS/xdelta3-ios-test/xdelta3-ios-test/";
        let source = read_file(&format!("{}/{}", fixure_path, "file_v1.bin"));
        let input = read_file(&format!("{}/{}", fixure_path, "file_v2.bin"));

        let patch_sync = encode(&input, &source).expect("failed to encode");
        assert_eq!(input, check_decode(&patch_sync, &source));

        let patch_async = encode2(&input, &source).expect("failed to encode");
        assert_eq!(input, check_decode(&patch_async, &source));
    }

    #[test]
    #[cfg(feature = "stream")]
    fn decode_stream_rejects_truncated_window() {
        let source: Vec<u8> = (0..1024)
            .map(|index| ((index * 31 + 7) % 256) as u8)
            .collect();
        let target = source[128..256].to_vec();
        let patch = encode(&target, &source).expect("failed to encode test patch");
        let truncated_patch = &patch[..patch.len() - 1];

        assert!(decode(truncated_patch, &source).is_err());

        let mut output = Vec::new();
        let result = futures::executor::block_on(try_decode_async(
            truncated_patch,
            source.as_slice(),
            &mut output,
        ));
        assert!(matches!(
            result,
            Err(StreamError::XDelta3 {
                code,
                message: Some(message)
            }) if code != 0 && message == "eof in decode"
        ));
    }

    #[test]
    #[cfg(feature = "stream")]
    fn decode_stream_preserves_empty_patch_ambiguity() {
        let header_only_patch = [0xd6, 0xc3, 0xc4, 0x00, 0x00];

        for patch in [&[][..], &header_only_patch] {
            let mut output = Vec::new();
            let result = futures::executor::block_on(try_decode_async(patch, &[][..], &mut output));

            assert!(result.is_ok());
            assert!(output.is_empty());
        }
    }

    #[test]
    #[cfg(feature = "stream")]
    fn stream_reports_input_and_source_read_errors() {
        let mut output = Vec::new();
        let input_error =
            futures::executor::block_on(try_encode_async(FailingReader, &[][..], &mut output));
        assert!(matches!(
            input_error,
            Err(StreamError::InputRead(error)) if error.to_string() == "reader failed"
        ));

        let source_error =
            futures::executor::block_on(try_encode_async(&[][..], FailingReader, &mut output));
        assert!(matches!(
            source_error,
            Err(StreamError::SourceRead(error)) if error.to_string() == "reader failed"
        ));
    }

    #[test]
    #[cfg(feature = "stream")]
    fn stream_reports_output_write_and_flush_errors() {
        let write_error = futures::executor::block_on(try_encode_async(
            &[0x5a][..],
            &[][..],
            TestWriter {
                fail_write: true,
                zero_write: false,
                fail_flush: false,
            },
        ));
        assert!(matches!(
            write_error,
            Err(StreamError::OutputWrite(error)) if error.to_string() == "writer failed"
        ));

        let zero_write_error = futures::executor::block_on(try_encode_async(
            &[0x5a][..],
            &[][..],
            TestWriter {
                fail_write: false,
                zero_write: true,
                fail_flush: false,
            },
        ));
        assert!(matches!(
            zero_write_error,
            Err(StreamError::OutputWrite(error)) if error.kind() == io::ErrorKind::WriteZero
        ));

        let flush_error = futures::executor::block_on(try_encode_async(
            &[0x5a][..],
            &[][..],
            TestWriter {
                fail_write: false,
                zero_write: false,
                fail_flush: true,
            },
        ));
        assert!(matches!(
            flush_error,
            Err(StreamError::OutputFlush(error)) if error.to_string() == "flush failed"
        ));
    }

    #[test]
    #[cfg(feature = "stream")]
    #[expect(
        deprecated,
        reason = "verify the legacy Option wrappers remain compatible"
    )]
    fn deprecated_stream_wrappers_preserve_option_behavior() {
        let source = b"the original file contents";
        let target = b"the updated file contents";
        let mut patch = Vec::new();

        let encode_result =
            futures::executor::block_on(encode_async(&target[..], &source[..], &mut patch));
        assert!(encode_result.is_some());

        let mut output = Vec::new();
        let decode_result =
            futures::executor::block_on(decode_async(&patch[..], &source[..], &mut output));
        assert!(decode_result.is_some());
        assert_eq!(output, target);

        let error_result =
            futures::executor::block_on(encode_async(FailingReader, &[][..], Vec::new()));
        assert!(error_result.is_none());
    }
}
