/***********************************************************************
* Filename : Compressor.cpp
* Create : XXX 2012-03-06
* Created Time: 2012-03-06
* Description: 
* Modified   : 
* **********************************************************************/

#include <zlib.h>
#include <LzmaLib.h>
#include "Compressor.h"

namespace arraydb {   
    CompressorFactory CompressorFactory::instance;
    CompressorFactory::CompressorFactory() {
	compressors.push_back(new NoCompression());
	compressors.push_back(new ZlibCompressor());
	compressors.push_back(new LzmaCompressor());
    }

    /*
     * zlib compressor
     */
    int ZlibCompressor::compressionLevel = Z_DEFAULT_COMPRESSION;

    size_t ZlibCompressor::compress(void *dest_buf, size_t chunk_size, const void *src_buf) {
	size_t size = chunk_size;
	uLongf dstLen = size;
	int rc = compress2((Bytef*)dest_buf, &dstLen, (Bytef*)src_buf, size, compressionLevel);
	return rc == Z_OK ? dstLen : 0;
    }

    size_t ZlibCompressor::decompress(void *dest_buf, size_t dest_size, const void *src_buf, size_t src_size) {
	uLongf dstLen = dest_size;
	int rc = uncompress((Bytef*)dest_buf, &dstLen, (Bytef*)src_buf, src_size);
	return rc == Z_OK ? dstLen : 0;
    }
    
    /*
     * lzma compressor
     */
    size_t LzmaCompressor::compress(void *dest_buf, size_t chunk_size, const void *src_buf) {
	int status;
	size_t destlen, propsize;
	unsigned char *propbuf;
	propsize = LZMA_PROPS_SIZE;
	destlen = chunk_size - propsize;
	propbuf = (unsigned char *)dest_buf;

	status = LzmaCompress(&(((unsigned char *)dest_buf)[propsize]), &destlen, (const unsigned char *)src_buf, chunk_size, propbuf, &propsize, -1, 0, -1, -1, -1, -1, 1);
	return (status == SZ_OK) ? destlen + propsize : 0;
    }

    size_t LzmaCompressor::decompress(void *dest_buf, size_t dest_size, const void *src_buf, size_t src_size) {
	int status;
	size_t destlen, propsize;
	const unsigned char *propbuf;
	
	propsize = LZMA_PROPS_SIZE;
	destlen = dest_size;
	propbuf = (unsigned char *)src_buf;

	status = LzmaUncompress((unsigned char *)dest_buf, &destlen, &(((unsigned char *)src_buf)[propsize]), &destlen, propbuf, propsize);
	return (status == SZ_OK) ? destlen : 0;
    }
}
