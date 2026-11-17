package partlog

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
