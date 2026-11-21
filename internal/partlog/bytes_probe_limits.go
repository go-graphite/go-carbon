package partlog

// Standalone limit shared by the imported dictionary codecs and offline probe.
const maxPayload = 8 << 20

// Outer journal record bytes, included in the offline accounting receipts.
const headerSize = 32
