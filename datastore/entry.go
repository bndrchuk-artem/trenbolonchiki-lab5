package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key      string
	value    string
	checksum [20]byte // Добавляем поле для SHA1 чексуммы
}

const (
	headerSize      = 4
	keyLengthSize   = 4
	valueLengthSize = 4
	checksumSize    = 20 // Размер SHA1 хеша
	totalHeaderSize = headerSize + keyLengthSize + valueLengthSize + checksumSize
)

func calculateEntryLength(key, value string) int64 {
	return int64(len(key) + len(value) + totalHeaderSize)
}

func (e *entry) GetLength() int64 {
	return calculateEntryLength(e.key, e.value)
}

// Вычисление SHA1 хеша значения
func (e *entry) calculateChecksum() [20]byte {
	return sha1.Sum([]byte(e.value))
}

// Проверка чексуммы
func (e *entry) verifyChecksum() error {
	expectedChecksum := sha1.Sum([]byte(e.value))
	if expectedChecksum != e.checksum {
		return fmt.Errorf("checksum mismatch: data corruption detected for key '%s'", e.key)
	}
	return nil
}

func (e *entry) Decode(data []byte) {
	keyLength := binary.LittleEndian.Uint32(data[headerSize:])

	keyStart := headerSize + keyLengthSize
	keyEnd := keyStart + int(keyLength)

	keyBytes := make([]byte, keyLength)
	copy(keyBytes, data[keyStart:keyEnd])
	e.key = string(keyBytes)

	valueStart := keyEnd
	valueLength := binary.LittleEndian.Uint32(data[valueStart:])

	valueDataStart := valueStart + valueLengthSize
	valueDataEnd := valueDataStart + int(valueLength)

	valueBytes := make([]byte, valueLength)
	copy(valueBytes, data[valueDataStart:valueDataEnd])
	e.value = string(valueBytes)

	// Читаем чексумму
	checksumStart := valueDataEnd
	copy(e.checksum[:], data[checksumStart:checksumStart+checksumSize])
}

// Обновленная функция чтения с проверкой чексуммы
func readValue(reader *bufio.Reader) (string, error) {
	headerBytes, err := reader.Peek(headerSize + keyLengthSize)
	if err != nil {
		return "", err
	}

	keySize := int(binary.LittleEndian.Uint32(headerBytes[headerSize:]))

	// Пропускаем заголовок и ключ
	bytesToSkip := headerSize + keyLengthSize + keySize
	_, err = reader.Discard(bytesToSkip)
	if err != nil {
		return "", err
	}

	// Читаем размер значения
	valueSizeBytes, err := reader.Peek(valueLengthSize)
	if err != nil {
		return "", err
	}

	valueSize := int(binary.LittleEndian.Uint32(valueSizeBytes))

	_, err = reader.Discard(valueLengthSize)
	if err != nil {
		return "", err
	}

	// Читаем значение
	valueData := make([]byte, valueSize)
	bytesRead, err := reader.Read(valueData)
	if err != nil {
		return "", err
	}

	if bytesRead != valueSize {
		return "", fmt.Errorf("incomplete value read: got %d bytes, expected %d", bytesRead, valueSize)
	}

	var storedChecksum [20]byte
	checksumBytesRead, err := reader.Read(storedChecksum[:])
	if err != nil {
		return "", fmt.Errorf("failed to read checksum: %w", err)
	}

	if checksumBytesRead != checksumSize {
		return "", fmt.Errorf("incomplete checksum read: got %d bytes, expected %d", checksumBytesRead, checksumSize)
	}

	// Проверяем чексумму
	expectedChecksum := sha1.Sum(valueData)
	if expectedChecksum != storedChecksum {
		return "", fmt.Errorf("checksum mismatch: data corruption detected")
	}

	return string(valueData), nil
}

// Обновленная функция кодирования с чексуммой
func (e *entry) Encode() []byte {
	e.checksum = e.calculateChecksum() // Вычисляем чексумму перед сохранением

	keyLength := len(e.key)
	valueLength := len(e.value)
	totalSize := keyLength + valueLength + totalHeaderSize

	buffer := make([]byte, totalSize)

	// Записываем общий размер
	binary.LittleEndian.PutUint32(buffer, uint32(totalSize))

	// Записываем размер ключа
	binary.LittleEndian.PutUint32(buffer[headerSize:], uint32(keyLength))

	// Записываем ключ
	copy(buffer[headerSize+keyLengthSize:], e.key)

	// Записываем размер значения
	valueStart := headerSize + keyLengthSize + keyLength
	binary.LittleEndian.PutUint32(buffer[valueStart:], uint32(valueLength))

	// Записываем значение
	copy(buffer[valueStart+valueLengthSize:], e.value)

	// Записываем чексумму
	checksumStart := valueStart + valueLengthSize + valueLength
	copy(buffer[checksumStart:], e.checksum[:])

	return buffer
}
