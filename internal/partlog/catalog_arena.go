package partlog

import "bytes"

// Definitions are immutable for a catalog epoch and live exactly as long as
// their bindings. Pack them into byte pages instead of a heap allocation per
// metric. Returned slices have no spare capacity; appending cannot overwrite
// a neighboring definition. Large definitions receive a dedicated allocation.
// This changes only the in-memory representation, never the durable format.
func (c *NameCatalog) copyDefinition(definition []byte) []byte {
	if len(definition) == 0 {
		return nil
	}
	const pageBytes = 64 << 10
	if len(definition) > pageBytes {
		return append([]byte(nil), definition...)
	}
	if cap(c.definitionArena)-len(c.definitionArena) < len(definition) {
		c.definitionArena = make([]byte, 0, pageBytes)
	}
	start := len(c.definitionArena)
	c.definitionArena = append(c.definitionArena, definition...)
	end := len(c.definitionArena)
	return c.definitionArena[start:end:end]
}

// Split definitions only when their leading name is exactly the catalog key.
// This is a lossless byte representation, independent of policy interpretation.
// Group/checkpoint encoders reconstruct the original bytes at the disk boundary.
const definitionNamePrefix = `{"Name":"`

func (e catalogEntry) definitionBytes() []byte {
	if !e.split {
		return e.definition
	}
	out := make([]byte, 0, len(definitionNamePrefix)+len(e.key)+len(e.definition))
	out = append(out, definitionNamePrefix...)
	out = append(out, e.key...)
	return append(out, e.definition...)
}
func (e catalogEntry) definitionEqual(raw []byte) bool {
	if !e.split {
		return bytes.Equal(e.definition, raw)
	}
	n := len(definitionNamePrefix) + len(e.key)
	return len(raw) == n+len(e.definition) && bytes.HasPrefix(raw, []byte(definitionNamePrefix)) && string(raw[len(definitionNamePrefix):n]) == e.key && bytes.Equal(raw[n:], e.definition)
}
func (c *NameCatalog) bindEntry(id uint64, name string, definition []byte) error {
	if c.binding != nil {
		if err := c.binding.Bind(id, name, definition); err != nil {
			return err
		}
		c.boundCount = id
		return nil
	}
	n := len(definitionNamePrefix) + len(name)
	// JSON escapes cannot compare equal to the literal key, so use generic storage.
	if len(definition) > n && bytes.HasPrefix(definition, []byte(definitionNamePrefix)) && string(definition[len(definitionNamePrefix):n]) == name && definition[n] == '"' {
		if c.definitionSuffixes == nil {
			c.definitionSuffixes = map[string][]byte{}
		}
		suffix := c.definitionSuffixes[string(definition[n:])]
		if suffix == nil {
			suffix = c.copyDefinition(definition[n:])
			c.definitionSuffixes[string(suffix)] = suffix
		}
		c.entries = append(c.entries, catalogEntry{key: name, definition: suffix, split: true})
		return nil
	}
	c.entries = append(c.entries, catalogEntry{key: name, definition: c.copyDefinition(definition)})
	return nil
}
