package middleware_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/middleware"
)

func TestEncryptHook_NewEncrypt_ValidKey16(t *testing.T) {
	key := make([]byte, 16)
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestEncryptHook_NewEncrypt_ValidKey24(t *testing.T) {
	key := make([]byte, 24)
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestEncryptHook_NewEncrypt_ValidKey32(t *testing.T) {
	key := make([]byte, 32)
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestEncryptHook_NewEncrypt_InvalidKey(t *testing.T) {
	key := make([]byte, 15)
	h, err := middleware.NewEncrypt(key)
	assert.Error(t, err)
	assert.Nil(t, h)
}

func TestEncryptHook_RoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef") // 16 bytes
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)

	plaintext := []byte("hello, encrypted world!")
	ciphertext, err := h.Encrypt(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, ciphertext, "ciphertext should differ from plaintext")

	decrypted, err := h.Decrypt(ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestEncryptHook_DecryptTooShort(t *testing.T) {
	key := []byte("0123456789abcdef")
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)

	// AES-GCM nonce is 12 bytes; data shorter than that should fail.
	short := []byte("tiny")
	_, err = h.Decrypt(short)
	assert.Error(t, err, "decrypt should fail when ciphertext is shorter than nonce size")
}

func TestEncryptHook_DecryptCorrupted(t *testing.T) {
	key := []byte("0123456789abcdef")
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)

	plaintext := []byte("sensitive data")
	ciphertext, err := h.Encrypt(plaintext)
	require.NoError(t, err)

	// Corrupt a byte in the ciphertext portion (after the nonce).
	corrupted := make([]byte, len(ciphertext))
	copy(corrupted, ciphertext)
	corrupted[len(corrupted)-1] ^= 0xFF

	_, err = h.Decrypt(corrupted)
	assert.Error(t, err, "decrypting corrupted ciphertext should fail")
}

func TestEncryptHook_DecryptWrongKey(t *testing.T) {
	key1 := []byte("0123456789abcdef")
	key2 := []byte("fedcba9876543210")

	h1, err := middleware.NewEncrypt(key1)
	require.NoError(t, err)
	h2, err := middleware.NewEncrypt(key2)
	require.NoError(t, err)

	plaintext := []byte("secret message")
	ciphertext, err := h1.Encrypt(plaintext)
	require.NoError(t, err)

	_, err = h2.Decrypt(ciphertext)
	assert.Error(t, err, "decrypting with wrong key should fail")
}

func TestEncryptHook_NonceUniqueness(t *testing.T) {
	key := []byte("0123456789abcdef")
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)

	plaintext := []byte("same data each time")
	ct1, err := h.Encrypt(plaintext)
	require.NoError(t, err)
	ct2, err := h.Encrypt(plaintext)
	require.NoError(t, err)

	assert.False(t, bytes.Equal(ct1, ct2), "encrypting the same plaintext twice should produce different ciphertexts due to random nonce")
}

func TestEncryptHook_EmptyPlaintext(t *testing.T) {
	key := []byte("0123456789abcdef")
	h, err := middleware.NewEncrypt(key)
	require.NoError(t, err)

	ciphertext, err := h.Encrypt([]byte{})
	require.NoError(t, err)
	assert.NotEmpty(t, ciphertext, "ciphertext should contain at least nonce + auth tag")

	decrypted, err := h.Decrypt(ciphertext)
	require.NoError(t, err)
	assert.Empty(t, decrypted, "decrypted empty plaintext should be empty")
}
