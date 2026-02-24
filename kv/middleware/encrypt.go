package middleware

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// EncryptHook provides transparent AES-GCM encryption for values at rest.
type EncryptHook struct {
	gcm cipher.AEAD
}

// NewEncrypt creates a new encryption middleware with the given AES key.
// The key must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256.
func NewEncrypt(key []byte) (*EncryptHook, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &EncryptHook{gcm: gcm}, nil
}

// Encrypt encrypts plaintext using AES-GCM.
func (h *EncryptHook) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, h.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return h.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext using AES-GCM.
func (h *EncryptHook) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := h.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("kv: ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return h.gcm.Open(nil, nonce, ciphertext, nil)
}
