/***********************************************************************
* Filename : Compressor.h
* Create : XXX 2012-03-06
* Created Time: 2012-03-06
* Description: 
* Modified   : 
* **********************************************************************/
#ifndef COMPRESSOR_H
#define COMPRESSOR_H
#include <string> 
#include <vector>

#include "arraydb_types.h"

using namespace std;

namespace arraydb {
    // Compressor interface
    class Compressor {
	public:
	    virtual size_t compress(void *dest_buf, size_t chunk_size, const void *src_buf) = 0;
	    virtual size_t decompress(void *dest_buf, size_t dest_size, const void *src_buf, size_t src_size) = 0;
	    virtual const char* getName() = 0;
	    virtual int16_t getType() const = 0;
    };

    class CompressorFactory {
	    std::vector<Compressor *> compressors;
	    static CompressorFactory instance;
	    
	    CompressorFactory();
	    ~CompressorFactory(){};
	    CompressorFactory(const CompressorFactory&);
	    CompressorFactory& operator = (const CompressorFactory&);

	public:

	    /*enum Compressors {
		NO_COMPRESSION = 0,
		ZLIB,
		LZMA
	    };*/

	    static const CompressorFactory& getInstance() {
		return instance;
	    }

	    const std::vector<Compressor *>& getCompressors() const {
		return compressors;
	    }

	    const Compressor* getCompressor(CompressType::type type) {
		return compressors[type];
	    }
    };

    class NoCompression: public Compressor {
    	public:
	    virtual const char* getName() {
		return "no_compression";
	    }
	    virtual size_t compress(void *dest_buf, size_t chunk_size, const void *src_buf){return 0;}
	    virtual size_t decompress(void *dest_buf, size_t dest_size, const void *src_buf, size_t src_size){return 0;} 
	    virtual int16_t getType() const {
		return CompressType::NO_COMPRESSION;
	    }

    };

    class ZlibCompressor: public Compressor {
	public:
	    static int compressionLevel;
	    virtual const char* getName() {
		return "zlib";
	    }
	    virtual size_t compress(void *dest_buf, size_t chunk_size, const void *src_buf);
	    virtual size_t decompress(void *dest_buf, size_t dest_size, const void *src_buf, size_t src_size);
	    virtual int16_t getType() const {
		return CompressType::ZLIB;
	    }
    };
    
    class LzmaCompressor: public Compressor {
    	public:
	    virtual const char* getName() {
		return "lzma";
	    }
	    virtual size_t compress(void *dest_buf, size_t chunk_size, const void *src_buf);
	    virtual size_t decompress(void *dest_buf, size_t dest_size, const void *src_buf, size_t src_size); 
	    virtual int16_t getType() const {
		return CompressType::LZMA;
	    }

    };

}

#endif
