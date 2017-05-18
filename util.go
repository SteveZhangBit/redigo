package redigo

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
