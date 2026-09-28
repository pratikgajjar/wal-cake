package transform

import (
	"sync"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/klauspost/compress/zstd"
)

// pooledZstd is arrow-go's ZSTD codec with reusable encoders.
//
// arrow-go v18.2.0 creates a new zstd.Encoder in EncodeLevel, which the
// column writer calls once per page. Each encoder allocates about 18 MB, so a
// 1,000-row file with 8 pages allocated about 152 MB. Reusing encoders
// produces the same bytes with about 2 MB of allocation.
type pooledZstd struct {
	compress.Codec
	pools sync.Map // zstd.EncoderLevel -> *sync.Pool of *zstd.Encoder
}

func (c *pooledZstd) EncodeLevel(dst, src []byte, level int) []byte {
	encLevel := zstd.SpeedDefault
	if level != compress.DefaultCompressionLevel {
		encLevel = zstd.EncoderLevelFromZstd(level)
	}
	pool := c.pool(encLevel)
	enc := pool.Get().(*zstd.Encoder)
	defer pool.Put(enc)
	return enc.EncodeAll(src, dst[:0])
}

func (c *pooledZstd) pool(level zstd.EncoderLevel) *sync.Pool {
	if p, ok := c.pools.Load(level); ok {
		return p.(*sync.Pool)
	}
	p, _ := c.pools.LoadOrStore(level, &sync.Pool{New: func() any {
		// Same options as arrow-go's EncodeLevel, so the output is identical.
		enc, err := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(level))
		if err != nil {
			panic(err) // only fails on invalid options
		}
		return enc
	}})
	return p.(*sync.Pool)
}

var registerPooledZstd = sync.OnceFunc(func() {
	codec, err := compress.GetCodec(compress.Codecs.Zstd)
	if err != nil {
		panic(err)
	}
	compress.RegisterCodec(compress.Codecs.Zstd, &pooledZstd{Codec: codec})
})
