package benchmark

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
)

func BenchmarkPackageBlock(b *testing.B) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	transaction := buildTransaction(privateKey)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transaction.Verify()
	}
}
