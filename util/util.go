package util

import (
	"unicode"
)

func StringMatchPattern(pattern, str string, nocase bool) bool {
	return MatchPattern([]byte(pattern), []byte(str), nocase)
}

func MatchPattern(pattern, str []byte, nocase bool) bool {
	var i, j int

	plen := len(pattern)
	slen := len(str)
	for i < plen {
		switch pattern[i] {
		case '*':
			for i+1 < plen && pattern[i+1] == '*' {
				i++
			}
			if i+1 == plen {
				return true
			}
			for ; j < slen; j++ {
				if MatchPattern(pattern[i+1:], str[j:], nocase) {
					return true
				}
			}
			return false
		case '?':
			if len(str) == 0 {
				return false
			}
			j++
		case '[':
			var not, match bool

			i++
			not = pattern[i] == '^'
			if not {
				i++
			}
			for {
				if pattern[i] == '\\' {
					i++
					if pattern[i] == str[j] {
						match = true
					}
				} else if pattern[i] == ']' {
					break
				} else if i == plen {
					i--
					break
				} else if pattern[i+1] == '-' && i+3 <= plen {
					start, end, c := pattern[i], pattern[i+2], pattern[i]
					if start > end {
						start, end = end, start
					}
					if nocase {
						start = byte(unicode.ToLower(rune(start)))
						end = byte(unicode.ToLower(rune(end)))
						c = byte(unicode.ToLower(rune(c)))
					}
					i += 2
					if c >= start && c <= end {
						match = true
					}
				} else {
					if !nocase {
						if pattern[i] == str[j] {
							match = true
						}
					} else {
						if unicode.ToLower(rune(pattern[i])) == unicode.ToLower(rune(str[j])) {
							match = true
						}
					}
				}
				i++
			}
			if not {
				match = !match
			}
			if !match {
				return false
			}
			j++
		case '\\':
			if i+2 <= plen {
				i++
			}
		default:
			if !nocase {
				if pattern[i] != str[j] {
					return false
				}
			} else {
				if unicode.ToLower(rune(pattern[i])) != unicode.ToLower(rune(str[j])) {
					return false
				}
			}
			j++
		}
		i++
		if j == slen {
			for i < plen && pattern[i] == '*' {
				i++
			}
			break
		}
	}
	return i == plen && j == slen
}

/* The following parse functions are the copy from go's standard library.
 * The idea is to avoid bytes to string convert, and the malloc in it.
 */
const intSize = 32 << (^uint(0) >> 63)

// IntSize is the size in bits of an int or uint value.
const IntSize = intSize

const maxUint64 = 1<<64 - 1

// ParseUint is like ParseInt but for unsigned numbers.
func ParseUint(s []byte, base int, bitSize int) (uint64, bool) {
	var n uint64
	var cutoff, maxVal uint64

	if bitSize == 0 {
		bitSize = int(IntSize)
	}

	i := 0
	switch {
	case len(s) < 1:
		goto Error

	case 2 <= base && base <= 36:
		// valid base; nothing to do

	case base == 0:
		// Look for octal, hex prefix.
		switch {
		case s[0] == '0' && len(s) > 1 && (s[1] == 'x' || s[1] == 'X'):
			if len(s) < 3 {
				goto Error
			}
			base = 16
			i = 2
		case s[0] == '0':
			base = 8
			i = 1
		default:
			base = 10
		}

	default:
		goto Error
	}

	// Cutoff is the smallest number such that cutoff*base > maxUint64.
	// Use compile-time constants for common cases.
	switch base {
	case 10:
		cutoff = maxUint64/10 + 1
	case 16:
		cutoff = maxUint64/16 + 1
	default:
		cutoff = maxUint64/uint64(base) + 1
	}

	maxVal = 1<<uint(bitSize) - 1

	for ; i < len(s); i++ {
		var v byte
		d := s[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		case 'a' <= d && d <= 'z':
			v = d - 'a' + 10
		case 'A' <= d && d <= 'Z':
			v = d - 'A' + 10
		default:
			n = 0
			goto Error
		}
		if v >= byte(base) {
			n = 0
			goto Error
		}

		if n >= cutoff {
			// n*base overflows
			n = maxUint64
			goto Error
		}
		n *= uint64(base)

		n1 := n + uint64(v)
		if n1 < n || n1 > maxVal {
			// n+v overflows
			n = maxUint64
			goto Error
		}
		n = n1
	}

	return n, true

Error:
	return n, false
}

func ParseInt(s []byte, base int, bitSize int) (i int64, ok bool) {
	if bitSize == 0 {
		bitSize = int(IntSize)
	}

	// Empty string bad.
	if len(s) == 0 {
		return 0, false
	}

	// Pick off leading sign.
	neg := false
	if s[0] == '+' {
		s = s[1:]
	} else if s[0] == '-' {
		neg = true
		s = s[1:]
	}

	// Convert unsigned and check range.
	var un uint64
	un, ok = ParseUint(s, base, bitSize)
	if !ok {
		return 0, false
	}
	cutoff := uint64(1 << uint(bitSize-1))
	if !neg && un >= cutoff {
		return int64(cutoff - 1), false
	}
	if neg && un > cutoff {
		return -int64(cutoff), false
	}
	n := int64(un)
	if neg {
		n = -n
	}
	return n, true
}

// Custom version of bytes.tolower

func ToLower(b []byte) []byte {
	const decr = 'a' - 'A'
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] += decr
		}
	}
	return b
}
