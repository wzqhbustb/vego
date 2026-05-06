package memory

import (
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ----------------------------------------------------------------------
// TemporalMetadata
// ----------------------------------------------------------------------

// TemporalMetadata holds resolved temporal information for a fact or query.
type TemporalMetadata struct {
	Kind          string `json:"kind"`           // explicit_absolute | deictic_relative | header_anchor_relative
	AnchorSource  string `json:"anchor_source"`  // local | header | now
	Granularity   string `json:"granularity"`    // day | week | month | year | season
	ResolvedStart string `json:"resolved_start"` // ISO 8601 date
	ResolvedEnd   string `json:"resolved_end"`   // ISO 8601 date (inclusive end for granularity > day)
	Display       string `json:"display"`        // human readable, e.g. "昨天"
}

// ----------------------------------------------------------------------
// Public API
// ----------------------------------------------------------------------

// NormalizeTemporalFacts replaces relative temporal expressions in facts
// with absolute dates. It extracts the anchor date from messages (falling
// back to "now") and resolves each deictic reference.
func NormalizeTemporalFacts(facts []ExtractedFact, messages []Message, now time.Time) []ExtractedFact {
	anchor, source := extractAnchorDate(messages, now)
	out := make([]ExtractedFact, len(facts))
	for i, f := range facts {
		out[i] = f
		resolved, meta := resolveInContent(f.Content, anchor, source)
		if meta != nil {
			out[i].Content = resolved
			out[i].Metadata = copyMap(f.Metadata)
			if out[i].Metadata == nil {
				out[i].Metadata = make(map[string]interface{})
			}
			out[i].Metadata["temporal"] = meta
		}
	}
	return out
}

// NormalizeTemporalRecallQuery normalizes relative time expressions in a
// search query so that vector/keyword search can match against absolute
// dates stored in memories.
func NormalizeTemporalRecallQuery(query string, now time.Time) string {
	resolved, _ := resolveInContent(query, now, "now")
	return resolved
}

// temporalMetadataFrom extracts display and resolved_start from a
// *TemporalMetadata (unit-test path) or map[string]interface{}
// (post-JSON-round-trip production path).
func temporalMetadataFrom(raw interface{}) (display, resolved string, ok bool) {
	switch tm := raw.(type) {
	case *TemporalMetadata:
		// Preserve original behaviour: *TemporalMetadata is always honoured
		// and empty-value handling is left to replaceResolvedDate.
		return tm.Display, tm.ResolvedStart, true
	case map[string]interface{}:
		display, _ = tm["display"].(string)
		resolved, _ = tm["resolved_start"].(string)
		return display, resolved, display != "" && resolved != ""
	}
	return "", "", false
}

// TemporalRecallProjection replaces absolute ISO dates in content with
// human-relative descriptions (e.g. "2026-04-21" → "昨天") for display.
// It prefers metadata-driven replacement when available.
func TemporalRecallProjection(content string, metadata map[string]interface{}, now time.Time) string {
	if metadata != nil {
		if raw, ok := metadata["temporal"]; ok {
			if display, resolved, ok := temporalMetadataFrom(raw); ok {
				return replaceResolvedDate(content, resolved, display)
			}
		}
	}
	// Fallback: scan for ISO dates and relativize them against now.
	return relativizeISODates(content, now)
}

// ----------------------------------------------------------------------
// Anchor extraction
// ----------------------------------------------------------------------

// extractAnchorDate picks the best anchor date for resolving relative time.
// Priority: first message with non-zero Timestamp > now.
// Returns the anchor time and its source label ("header" or "now").
func extractAnchorDate(messages []Message, now time.Time) (time.Time, string) {
	for _, m := range messages {
		if !m.Timestamp.IsZero() {
			return m.Timestamp, "header"
		}
	}
	return now, "now"
}

// ----------------------------------------------------------------------
// Core resolution engine
// ----------------------------------------------------------------------

type timeUnit int

const (
	unitDay timeUnit = iota
	unitWeek
	unitMonth
	unitYear
	unitSeason
	unitWeekday
)

type relativePattern struct {
	re      *regexp.Regexp
	offset  int    // offset in units
	unit    timeUnit
	display string // original display text for metadata
}

var (
	// Chinese relative time patterns (no \b because Go's \b is ASCII-only).
	// Longer phrases are placed before shorter ones to avoid partial matches.
	cnPatterns = []relativePattern{
		{re: regexp.MustCompile(`上季度`), offset: -1, unit: unitSeason, display: "上季度"},
		{re: regexp.MustCompile(`下季度`), offset: 1, unit: unitSeason, display: "下季度"},
		{re: regexp.MustCompile(`本季度`), offset: 0, unit: unitSeason, display: "本季度"},
		{re: regexp.MustCompile(`上个月`), offset: -1, unit: unitMonth, display: "上个月"},
		{re: regexp.MustCompile(`下个月`), offset: 1, unit: unitMonth, display: "下个月"},
		{re: regexp.MustCompile(`本月`), offset: 0, unit: unitMonth, display: "本月"},
		{re: regexp.MustCompile(`上周`), offset: -1, unit: unitWeek, display: "上周"},
		{re: regexp.MustCompile(`下周`), offset: 1, unit: unitWeek, display: "下周"},
		{re: regexp.MustCompile(`本周`), offset: 0, unit: unitWeek, display: "本周"},
		{re: regexp.MustCompile(`前年`), offset: -2, unit: unitYear, display: "前年"},
		{re: regexp.MustCompile(`去年`), offset: -1, unit: unitYear, display: "去年"},
		{re: regexp.MustCompile(`明年`), offset: 1, unit: unitYear, display: "明年"},
		{re: regexp.MustCompile(`今年`), offset: 0, unit: unitYear, display: "今年"},
		{re: regexp.MustCompile(`前天`), offset: -2, unit: unitDay, display: "前天"},
		{re: regexp.MustCompile(`后天`), offset: 2, unit: unitDay, display: "后天"},
		{re: regexp.MustCompile(`昨天`), offset: -1, unit: unitDay, display: "昨天"},
		{re: regexp.MustCompile(`明天`), offset: 1, unit: unitDay, display: "明天"},
		{re: regexp.MustCompile(`今天`), offset: 0, unit: unitDay, display: "今天"},
	}

	// English relative time patterns (case-insensitive).
	// Longer phrases are placed before shorter ones to avoid partial matches
	// (e.g. "the day before yesterday" contains "yesterday").
	enPatterns = []relativePattern{
		{re: regexp.MustCompile(`(?i)\bthe day before yesterday\b`), offset: -2, unit: unitDay, display: "the day before yesterday"},
		{re: regexp.MustCompile(`(?i)\bthe day after tomorrow\b`), offset: 2, unit: unitDay, display: "the day after tomorrow"},
		{re: regexp.MustCompile(`(?i)\blast season\b`), offset: -1, unit: unitSeason, display: "last season"},
		{re: regexp.MustCompile(`(?i)\bnext season\b`), offset: 1, unit: unitSeason, display: "next season"},
		{re: regexp.MustCompile(`(?i)\bthis season\b`), offset: 0, unit: unitSeason, display: "this season"},
		{re: regexp.MustCompile(`(?i)\blast month\b`), offset: -1, unit: unitMonth, display: "last month"},
		{re: regexp.MustCompile(`(?i)\bnext month\b`), offset: 1, unit: unitMonth, display: "next month"},
		{re: regexp.MustCompile(`(?i)\bthis month\b`), offset: 0, unit: unitMonth, display: "this month"},
		{re: regexp.MustCompile(`(?i)\blast week\b`), offset: -1, unit: unitWeek, display: "last week"},
		{re: regexp.MustCompile(`(?i)\bnext week\b`), offset: 1, unit: unitWeek, display: "next week"},
		{re: regexp.MustCompile(`(?i)\bthis week\b`), offset: 0, unit: unitWeek, display: "this week"},
		{re: regexp.MustCompile(`(?i)\blast year\b`), offset: -1, unit: unitYear, display: "last year"},
		{re: regexp.MustCompile(`(?i)\bnext year\b`), offset: 1, unit: unitYear, display: "next year"},
		{re: regexp.MustCompile(`(?i)\bthis year\b`), offset: 0, unit: unitYear, display: "this year"},
		{re: regexp.MustCompile(`(?i)\byesterday\b`), offset: -1, unit: unitDay, display: "yesterday"},
		{re: regexp.MustCompile(`(?i)\btomorrow\b`), offset: 1, unit: unitDay, display: "tomorrow"},
		{re: regexp.MustCompile(`(?i)\btoday\b`), offset: 0, unit: unitDay, display: "today"},
	}

	// Chinese weekday patterns (e.g. 上周一, 本周三, 下周天).
	// Ordered by length descending to avoid partial matches.
	cnWeekdayPatterns = []relativePattern{
		{re: regexp.MustCompile(`大上周[一二三四五六日天]`), offset: -2, unit: unitWeekday, display: "大上周X"},
		{re: regexp.MustCompile(`上上周[一二三四五六日天]`), offset: -2, unit: unitWeekday, display: "上上周X"},
		{re: regexp.MustCompile(`大下周[一二三四五六日天]`), offset: 2, unit: unitWeekday, display: "大下周X"},
		{re: regexp.MustCompile(`下下周[一二三四五六日天]`), offset: 2, unit: unitWeekday, display: "下下周X"},
		{re: regexp.MustCompile(`上周[一二三四五六日天]`), offset: -1, unit: unitWeekday, display: "上周X"},
		{re: regexp.MustCompile(`本周[一二三四五六日天]`), offset: 0, unit: unitWeekday, display: "本周X"},
		{re: regexp.MustCompile(`下周[一二三四五六日天]`), offset: 1, unit: unitWeekday, display: "下周X"},
	}

	// English weekday patterns (e.g. last Monday, this Friday, next Sunday).
	enWeekdayPatterns = []relativePattern{
		{re: regexp.MustCompile(`(?i)\blast\s+(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b`), offset: -1, unit: unitWeekday, display: "last X"},
		{re: regexp.MustCompile(`(?i)\bthis\s+(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b`), offset: 0, unit: unitWeekday, display: "this X"},
		{re: regexp.MustCompile(`(?i)\bnext\s+(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b`), offset: 1, unit: unitWeekday, display: "next X"},
	}

	// English named-season patterns (e.g. last summer, this winter, next spring).
	enNamedSeasonPatterns = []relativePattern{
		{re: regexp.MustCompile(`(?i)\blast\s+(summer|winter|spring|fall|autumn)\b`), offset: -1, unit: unitSeason, display: "last X"},
		{re: regexp.MustCompile(`(?i)\bthis\s+(summer|winter|spring|fall|autumn)\b`), offset: 0, unit: unitSeason, display: "this X"},
		{re: regexp.MustCompile(`(?i)\bnext\s+(summer|winter|spring|fall|autumn)\b`), offset: 1, unit: unitSeason, display: "next X"},
	}

	// Absolute date patterns (detect but do not replace).
	isoDateRE = regexp.MustCompile(`\b(\d{4})-(\d{1,2})-(\d{1,2})\b`)
	cnDateRE1 = regexp.MustCompile(`(\d{4})年(\d{1,2})月(\d{1,2})日`)
	cnDateRE2 = regexp.MustCompile(`(\d{1,2})月(\d{1,2})日`)
	enDateRE  = regexp.MustCompile(`\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b`)

	// Local-anchor-relative patterns (used in scanLocalAnchorRelative / scanAnchorPeriod).
	cnLocalAnchorRE = regexp.MustCompile(`(\d{4})年(\d{1,2})月(\d{1,2})日\s*的(后|前)([一二两三四五六七八九十\d]+)?(天|周|星期|个月|年)`)
	enLocalAnchorRE = regexp.MustCompile(`(?i)(\d{4})-(\d{1,2})-(\d{1,2})\s+the\s+(next|previous)\s+(day|week|month|year)`)
	anchorPeriodRE  = regexp.MustCompile(`(?i)\bthe\s+(day|week|month|year)\s+(before|after)\s+(\d{4})-(\d{1,2})-(\d{1,2})\b`)
)

// validCNBoundary checks that a Chinese pattern match is not a substring
// of a larger Chinese word (e.g. "今年" inside "今年轻人").
//
// We only block a small set of high-confidence false-positives.  Aggressive
// boundary checks (e.g. rejecting any match followed by a CJK character)
// would silently drop far more valid matches ("用户昨天遇到" → "昨天" would
// be rejected because "遇" is CJK).
func validCNBoundary(content string, start, end int) bool {
	matched := content[start:end]

	// Known false-positives where a temporal expression is part of a larger
	// compound word with a different natural segmentation.
	if end < len(content) {
		r, size := utf8.DecodeRuneInString(content[end:])
		if r == utf8.RuneError {
			return true
		}
		switch matched {
		case "今年", "去年", "前年", "明年":
			// "今年轻人" → natural seg: "今" + "年轻人"
			if r == '轻' && size > 0 && end+size < len(content) {
				r2, _ := utf8.DecodeRuneInString(content[end+size:])
				if r2 == '人' || r2 == '女' {
					return false
				}
			}
		case "上个月", "下个月", "本月":
			// "上个月饼" → natural seg: "上个" + "月饼"
			if r == '饼' {
				return false
			}
		}
	}
	return true
}

// match holds a single temporal expression match within content.
type temporalMatch struct {
	start, end int
	iso        string
	meta       *TemporalMetadata
}

// resolveInContent scans content for temporal expressions and replaces them
// with absolute ISO dates. All matching patterns are replaced (not just the
// first). Returns the resolved content and metadata for the first match (nil
// if no temporal expressions found).
func resolveInContent(content string, anchor time.Time, anchorSrc string) (string, *TemporalMetadata) {
	var matches []temporalMatch

	// 1. Scan Chinese weekday patterns first (more specific than plain 上周/本周/下周).
	for i := range cnWeekdayPatterns {
		p := &cnWeekdayPatterns[i]
		locs := p.re.FindAllStringIndex(content, -1)
		for _, loc := range locs {
			if !validCNBoundary(content, loc[0], loc[1]) {
				continue
			}
			wd := parseCNWeekday(content[loc[0]:loc[1]])
			t := resolveWeekday(anchor, p.offset, wd)
			matches = append(matches, temporalMatch{
				start: loc[0],
				end:   loc[1],
				iso:   t.Format("2006-01-02"),
				meta:  buildWeekdayMeta(p, anchor, anchorSrc, wd),
			})
		}
	}

	// 2. Scan Chinese base patterns (e.g. 昨天, 上周, 本月).
	for i := range cnPatterns {
		p := &cnPatterns[i]
		locs := p.re.FindAllStringIndex(content, -1)
		for _, loc := range locs {
			if !validCNBoundary(content, loc[0], loc[1]) {
				continue
			}
			t := resolveAbsolute(anchor, p.offset, p.unit)
			matches = append(matches, temporalMatch{
				start: loc[0],
				end:   loc[1],
				iso:   t.Format("2006-01-02"),
				meta:  buildMeta(p, anchor, anchorSrc),
			})
		}
	}

	// 3. Scan English weekday patterns first (more specific than plain last/next week).
	for i := range enWeekdayPatterns {
		p := &enWeekdayPatterns[i]
		locs := p.re.FindAllStringIndex(content, -1)
		for _, loc := range locs {
			wd := parseENWeekday(content[loc[0]:loc[1]])
			t := resolveWeekday(anchor, p.offset, wd)
			matches = append(matches, temporalMatch{
				start: loc[0],
				end:   loc[1],
				iso:   t.Format("2006-01-02"),
				meta:  buildWeekdayMeta(p, anchor, anchorSrc, wd),
			})
		}
	}

	// 4. Scan English named-season patterns (more specific than plain season).
	for i := range enNamedSeasonPatterns {
		p := &enNamedSeasonPatterns[i]
		locs := p.re.FindAllStringIndex(content, -1)
		for _, loc := range locs {
			seasonName := parseSeasonName(content[loc[0]:loc[1]])
			start, end := resolveSeasonNamed(anchor, seasonName, p.offset)
			matches = append(matches, temporalMatch{
				start: loc[0],
				end:   loc[1],
				iso:   start.Format("2006-01-02"),
				meta:  buildSeasonMeta(p, anchor, anchorSrc, seasonName, start, end),
			})
		}
	}

	// 5. Scan English base patterns.
	for i := range enPatterns {
		p := &enPatterns[i]
		locs := p.re.FindAllStringIndex(content, -1)
		for _, loc := range locs {
			t := resolveAbsolute(anchor, p.offset, p.unit)
			matches = append(matches, temporalMatch{
				start: loc[0],
				end:   loc[1],
				iso:   t.Format("2006-01-02"),
				meta:  buildMeta(p, anchor, anchorSrc),
			})
		}
	}

	// 2.7. Scan local-anchor-relative patterns (e.g. "2025年4月13日的后一天").
	localMatches := scanLocalAnchorRelative(content, anchor, anchorSrc)
	matches = append(matches, localMatches...)

	// 2.8. Scan anchor-period patterns (e.g. "the week before 2026-04-14").
	periodMatches := scanAnchorPeriod(content, anchor, anchorSrc)
	matches = append(matches, periodMatches...)

	// 3. Replace all relative matches.
	if len(matches) > 0 {
		sort.Slice(matches, func(i, j int) bool {
			if matches[i].start != matches[j].start {
				return matches[i].start < matches[j].start
			}
			// Same start: prefer longer match so that dedup keeps the more
			// specific pattern (e.g. "上周五" over "上周").
			return matches[i].end > matches[j].end
		})

		// Remove overlaps: keep earlier match, skip later overlapping ones.
		var deduped []temporalMatch
		lastEnd := -1
		for _, m := range matches {
			if m.start < lastEnd {
				continue // overlaps with previous
			}
			deduped = append(deduped, m)
			lastEnd = m.end
		}

		// Replace from back to front to preserve positions.
		resolved := content
		for i := len(deduped) - 1; i >= 0; i-- {
			m := deduped[i]
			resolved = resolved[:m.start] + m.iso + resolved[m.end:]
		}
		return resolved, deduped[0].meta
	}

	// 4. Check for absolute dates — record metadata but do not replace.
	if m := isoDateRE.FindStringSubmatch(content); m != nil {
		if meta := parseAbsoluteMeta(m[1], m[2], m[3], m[0]); meta != nil {
			return content, meta
		}
	}
	if m := cnDateRE1.FindStringSubmatch(content); m != nil {
		if meta := parseAbsoluteMeta(m[1], m[2], m[3], m[0]); meta != nil {
			return content, meta
		}
	}
	if m := cnDateRE2.FindStringSubmatch(content); m != nil {
		// Use anchor year as fallback for month-day-only expressions.
		// Heuristic: pick the year (anchor-1, anchor, anchor+1) that produces
		// the date closest to the anchor.
		mo, _ := strconv.Atoi(m[1])
		d, _ := strconv.Atoi(m[2])
		var best time.Time
		bestDist := time.Duration(1<<63 - 1)
		for _, cy := range []int{anchor.Year() - 1, anchor.Year(), anchor.Year() + 1} {
			if ct, ok := validDate(cy, mo, d); ok {
				dist := ct.Sub(anchor)
				if dist < 0 {
					dist = -dist
				}
				if dist < bestDist {
					bestDist = dist
					best = ct
				}
			}
		}
		if !best.IsZero() {
			meta := parseAbsoluteMeta(strconv.Itoa(best.Year()), m[1], m[2], m[0])
			if meta != nil {
				return content, meta
			}
		}
	}
	if m := enDateRE.FindStringSubmatch(content); m != nil {
		if y, mo, d, ok := parseEnglishDate(m[1], m[2], m[3]); ok {
			if t, ok := validDate(y, mo, d); ok {
				meta := &TemporalMetadata{
					Kind:          "explicit_absolute",
					AnchorSource:  "",
					Granularity:   "day",
					ResolvedStart: t.Format("2006-01-02"),
					ResolvedEnd:   t.Format("2006-01-02"),
					Display:       m[0],
				}
				return content, meta
			}
		}
	}

	return content, nil
}

// buildMeta constructs TemporalMetadata from a matched pattern.
func buildMeta(p *relativePattern, anchor time.Time, anchorSrc string) *TemporalMetadata {
	resolved := resolveAbsolute(anchor, p.offset, p.unit)
	iso := resolved.Format("2006-01-02")
	kind := "deictic_relative"
	if anchorSrc == "header" {
		kind = "header_anchor_relative"
	}
	meta := &TemporalMetadata{
		Kind:          kind,
		AnchorSource:  anchorSrc,
		Granularity:   granularityString(p.unit),
		ResolvedStart: iso,
		ResolvedEnd:   iso,
		Display:       p.display,
	}
	if p.unit != unitDay {
		meta.ResolvedEnd = granularityEnd(resolved, p.unit).Format("2006-01-02")
	}
	return meta
}

// buildWeekdayMeta constructs TemporalMetadata for a weekday pattern.
func buildWeekdayMeta(p *relativePattern, anchor time.Time, anchorSrc string, wd time.Weekday) *TemporalMetadata {
	resolved := resolveWeekday(anchor, p.offset, wd)
	iso := resolved.Format("2006-01-02")
	kind := "deictic_relative"
	if anchorSrc == "header" {
		kind = "header_anchor_relative"
	}
	meta := &TemporalMetadata{
		Kind:          kind,
		AnchorSource:  anchorSrc,
		Granularity:   "day",
		ResolvedStart: iso,
		ResolvedEnd:   iso,
		Display:       p.display,
	}
	return meta
}

// buildSeasonMeta constructs TemporalMetadata for a named-season pattern.
func buildSeasonMeta(p *relativePattern, anchor time.Time, anchorSrc string, seasonName string, start, end time.Time) *TemporalMetadata {
	kind := "deictic_relative"
	if anchorSrc == "header" {
		kind = "header_anchor_relative"
	}
	return &TemporalMetadata{
		Kind:          kind,
		AnchorSource:  anchorSrc,
		Granularity:   "season",
		ResolvedStart: start.Format("2006-01-02"),
		ResolvedEnd:   end.Format("2006-01-02"),
		Display:       p.display,
	}
}

// scanLocalAnchorRelative scans for text-internal-anchor relative expressions
// such as "2025年4月13日的后一天" or "April 14, 2025 the next day".
func scanLocalAnchorRelative(content string, anchor time.Time, anchorSrc string) []temporalMatch {
	var matches []temporalMatch

	// Chinese: YYYY年M月D日的(后|前)(一|两|几)?(天|周|星期|个月|年)
	for _, m := range cnLocalAnchorRE.FindAllStringSubmatchIndex(content, -1) {
		y, _ := strconv.Atoi(content[m[2]:m[3]])
		mo, _ := strconv.Atoi(content[m[4]:m[5]])
		d, _ := strconv.Atoi(content[m[6]:m[7]])
		direction := content[m[8]:m[9]]
		quantityStr := ""
		if m[10] >= 0 {
			quantityStr = content[m[10]:m[11]]
		}
		unitStr := content[m[12]:m[13]]

		t, ok := validDate(y, mo, d)
		if !ok {
			continue
		}

		qty := parseCNQuantity(quantityStr)
		if qty <= 0 {
			qty = 1
		}
		if direction == "前" {
			qty = -qty
		}

		var resolved time.Time
		switch unitStr {
		case "天":
			resolved = t.AddDate(0, 0, qty)
		case "周", "星期":
			resolved = t.AddDate(0, 0, qty*7)
		case "个月":
			resolved = t.AddDate(0, qty, 0)
		case "年":
			resolved = t.AddDate(qty, 0, 0)
		}

		meta := &TemporalMetadata{
			Kind:          "local_anchor_relative",
			AnchorSource:  "local",
			Granularity:   "day",
			ResolvedStart: resolved.Format("2006-01-02"),
			ResolvedEnd:   resolved.Format("2006-01-02"),
			Display:       content[m[0]:m[1]],
		}
		matches = append(matches, temporalMatch{
			start: m[0],
			end:   m[1],
			iso:   resolved.Format("2006-01-02"),
			meta:  meta,
		})
	}

	// English: ISO date + "the (next|previous) day/week/month/year"
	for _, m := range enLocalAnchorRE.FindAllStringSubmatchIndex(content, -1) {
		y, _ := strconv.Atoi(content[m[2]:m[3]])
		mo, _ := strconv.Atoi(content[m[4]:m[5]])
		d, _ := strconv.Atoi(content[m[6]:m[7]])
		direction := strings.ToLower(content[m[8]:m[9]])
		unitStr := strings.ToLower(content[m[10]:m[11]])

		t, ok := validDate(y, mo, d)
		if !ok {
			continue
		}

		qty := 1
		if direction == "previous" {
			qty = -1
		}

		var resolved time.Time
		switch unitStr {
		case "day":
			resolved = t.AddDate(0, 0, qty)
		case "week":
			resolved = t.AddDate(0, 0, qty*7)
		case "month":
			resolved = t.AddDate(0, qty, 0)
		case "year":
			resolved = t.AddDate(qty, 0, 0)
		}

		meta := &TemporalMetadata{
			Kind:          "local_anchor_relative",
			AnchorSource:  "local",
			Granularity:   "day",
			ResolvedStart: resolved.Format("2006-01-02"),
			ResolvedEnd:   resolved.Format("2006-01-02"),
			Display:       content[m[0]:m[1]],
		}
		matches = append(matches, temporalMatch{
			start: m[0],
			end:   m[1],
			iso:   resolved.Format("2006-01-02"),
			meta:  meta,
		})
	}

	return matches
}

// scanAnchorPeriod scans for anchor-period expressions such as
// "the week before 2026-04-14" or "the month after 2025-12-01".
func scanAnchorPeriod(content string, anchor time.Time, anchorSrc string) []temporalMatch {
	var matches []temporalMatch

	for _, m := range anchorPeriodRE.FindAllStringSubmatchIndex(content, -1) {
		unitStr := strings.ToLower(content[m[2]:m[3]])
		direction := strings.ToLower(content[m[4]:m[5]])
		y, _ := strconv.Atoi(content[m[6]:m[7]])
		mo, _ := strconv.Atoi(content[m[8]:m[9]])
		d, _ := strconv.Atoi(content[m[10]:m[11]])

		t, ok := validDate(y, mo, d)
		if !ok {
			continue
		}

		qty := 1
		if direction == "before" {
			qty = -1
		}

		var resolved time.Time
		switch unitStr {
		case "day":
			resolved = t.AddDate(0, 0, qty)
		case "week":
			resolved = t.AddDate(0, 0, qty*7)
		case "month":
			resolved = t.AddDate(0, qty, 0)
		case "year":
			resolved = t.AddDate(qty, 0, 0)
		}

		meta := &TemporalMetadata{
			Kind:          "local_anchor_relative",
			AnchorSource:  "local",
			Granularity:   "day",
			ResolvedStart: resolved.Format("2006-01-02"),
			ResolvedEnd:   resolved.Format("2006-01-02"),
			Display:       content[m[0]:m[1]],
		}
		matches = append(matches, temporalMatch{
			start: m[0],
			end:   m[1],
			iso:   resolved.Format("2006-01-02"),
			meta:  meta,
		})
	}

	return matches
}

// parseCNQuantity parses a simple Chinese quantity string (e.g. "一" → 1, "两" → 2).
// It falls back to strconv.Atoi for numeric strings.  Returns 0 if unrecognised.
func parseCNQuantity(s string) int {
	s = strings.TrimSpace(s)
	switch s {
	case "一", "1":
		return 1
	case "两", "二", "2":
		return 2
	case "三", "3":
		return 3
	case "四", "4":
		return 4
	case "五", "5":
		return 5
	case "六", "6":
		return 6
	case "七", "7":
		return 7
	case "八", "8":
		return 8
	case "九", "9":
		return 9
	case "十", "10":
		return 10
	case "几":
		return 1 // fallback for "几天"
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return 0
}

func parseAbsoluteMeta(yearStr, monthStr, dayStr, display string) *TemporalMetadata {
	y, _ := strconv.Atoi(yearStr)
	mo, _ := strconv.Atoi(monthStr)
	d, _ := strconv.Atoi(dayStr)
	if t, ok := validDate(y, mo, d); ok {
		return &TemporalMetadata{
			Kind:          "explicit_absolute",
			AnchorSource:  "",
			Granularity:   "day",
			ResolvedStart: t.Format("2006-01-02"),
			ResolvedEnd:   t.Format("2006-01-02"),
			Display:       display,
		}
	}
	return nil
}

func parseEnglishDate(monthName, dayStr, yearStr string) (y, mo, d int, ok bool) {
	months := map[string]int{
		"January": 1, "February": 2, "March": 3, "April": 4,
		"May": 5, "June": 6, "July": 7, "August": 8,
		"September": 9, "October": 10, "November": 11, "December": 12,
	}
	mo, ok = months[monthName]
	if !ok {
		return 0, 0, 0, false
	}
	d, err := strconv.Atoi(dayStr)
	if err != nil {
		slog.Debug("parseEnglishDate: invalid day", "day", dayStr, "err", err)
		return 0, 0, 0, false
	}
	y, err = strconv.Atoi(yearStr)
	if err != nil {
		slog.Debug("parseEnglishDate: invalid year", "year", yearStr, "err", err)
		return 0, 0, 0, false
	}
	return y, mo, d, true
}

// ----------------------------------------------------------------------
// Weekday / Season helpers
// ----------------------------------------------------------------------

// parseCNWeekday extracts the target weekday from a Chinese weekday pattern
// match (e.g. "上周一" → time.Monday).  It handles 日/天 as Sunday.
func parseCNWeekday(s string) time.Weekday {
	// The last rune is the weekday character.
	runes := []rune(s)
	if len(runes) == 0 {
		return time.Sunday
	}
	switch runes[len(runes)-1] {
	case '一':
		return time.Monday
	case '二':
		return time.Tuesday
	case '三':
		return time.Wednesday
	case '四':
		return time.Thursday
	case '五':
		return time.Friday
	case '六':
		return time.Saturday
	case '日', '天':
		return time.Sunday
	}
	return time.Sunday
}

// parseENWeekday extracts the target weekday from an English weekday pattern
// match (e.g. "last Monday" → time.Monday).
func parseENWeekday(s string) time.Weekday {
	s = strings.ToLower(s)
	switch {
	case strings.Contains(s, "monday"):
		return time.Monday
	case strings.Contains(s, "tuesday"):
		return time.Tuesday
	case strings.Contains(s, "wednesday"):
		return time.Wednesday
	case strings.Contains(s, "thursday"):
		return time.Thursday
	case strings.Contains(s, "friday"):
		return time.Friday
	case strings.Contains(s, "saturday"):
		return time.Saturday
	case strings.Contains(s, "sunday"):
		return time.Sunday
	}
	return time.Sunday
}

// resolveWeekday returns the target weekday in the weekOffset week relative to
// anchor's week.  weekOffset: -1=last week, 0=this week, 1=next week.
func resolveWeekday(anchor time.Time, weekOffset int, targetWeekday time.Weekday) time.Time {
	// Days since Monday for the anchor.
	daysSinceMonday := (int(anchor.Weekday()) - 1 + 7) % 7
	thisMonday := anchor.AddDate(0, 0, -daysSinceMonday)
	targetMonday := thisMonday.AddDate(0, 0, weekOffset*7)
	daysFromMonday := (int(targetWeekday) - 1 + 7) % 7
	return targetMonday.AddDate(0, 0, daysFromMonday)
}

// seasonMap maps English season names to their start month/day and end month/day.
var seasonMap = map[string]struct {
	startMonth time.Month
	startDay   int
	endMonth   time.Month
	endDay     int
}{
	"summer":  {time.June, 1, time.August, 31},
	"winter":  {time.December, 1, time.February, 28},
	"spring":  {time.March, 1, time.May, 31},
	"fall":    {time.September, 1, time.November, 30},
	"autumn":  {time.September, 1, time.November, 30},
}

// parseSeasonName extracts the season name from a matched English season
// pattern (e.g. "last summer" → "summer").
func parseSeasonName(s string) string {
	s = strings.ToLower(s)
	for name := range seasonMap {
		if strings.Contains(s, name) {
			return name
		}
	}
	return ""
}

// resolveSeasonNamed returns the start and end dates for a named season
// with a year offset relative to anchor.  Simplified: offset is applied
// directly to anchor's year.
func resolveSeasonNamed(anchor time.Time, seasonName string, yearOffset int) (start, end time.Time) {
	info := seasonMap[strings.ToLower(seasonName)]
	year := anchor.Year() + yearOffset
	start = time.Date(year, info.startMonth, info.startDay, 0, 0, 0, 0, time.UTC)
	if info.endMonth < info.startMonth {
		// Winter spans two years.
		end = time.Date(year+1, info.endMonth, info.endDay, 0, 0, 0, 0, time.UTC)
	} else {
		end = time.Date(year, info.endMonth, info.endDay, 0, 0, 0, 0, time.UTC)
	}
	return start, end
}

// ----------------------------------------------------------------------
// Date arithmetic
// ----------------------------------------------------------------------

func resolveAbsolute(anchor time.Time, offset int, unit timeUnit) time.Time {
	switch unit {
	case unitDay:
		return anchor.AddDate(0, 0, offset)
	case unitWeek:
		return anchor.AddDate(0, 0, offset*7)
	case unitMonth:
		return anchor.AddDate(0, offset, 0)
	case unitYear:
		return anchor.AddDate(offset, 0, 0)
	case unitSeason:
		return anchor.AddDate(0, offset*3, 0)
	default:
		return anchor
	}
}

func granularityEnd(start time.Time, unit timeUnit) time.Time {
	switch unit {
	case unitWeek, unitWeekday:
		return start.AddDate(0, 0, 6)
	case unitMonth:
		return start.AddDate(0, 1, -1)
	case unitYear:
		return start.AddDate(1, 0, -1)
	case unitSeason:
		return start.AddDate(0, 3, -1)
	default:
		return start
	}
}

func granularityString(u timeUnit) string {
	switch u {
	case unitDay:
		return "day"
	case unitWeek, unitWeekday:
		return "week"
	case unitMonth:
		return "month"
	case unitYear:
		return "year"
	case unitSeason:
		return "season"
	default:
		return ""
	}
}

func validDate(y, m, d int) (time.Time, bool) {
	t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	if t.Year() != y || int(t.Month()) != m || t.Day() != d {
		return time.Time{}, false
	}
	return t, true
}

// ----------------------------------------------------------------------
// Projection helpers
// ----------------------------------------------------------------------

func replaceResolvedDate(content, isoDate, display string) string {
	if isoDate == "" || display == "" {
		return content
	}
	return strings.ReplaceAll(content, isoDate, display)
}

func relativizeISODates(content string, now time.Time) string {
	return isoDateRE.ReplaceAllStringFunc(content, func(iso string) string {
		t, err := time.Parse("2006-01-02", iso)
		if err != nil {
			return iso
		}
		return humanRelative(t, now)
	})
}

// calendarDaysDiff returns the number of calendar days between t and now.
// Positive means t is in the past; negative means t is in the future.
func calendarDaysDiff(t, now time.Time) int {
	t0 := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	n0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return int(n0.Sub(t0).Hours() / 24)
}

// humanRelative returns a human-readable relative description.
func humanRelative(t, now time.Time) string {
	days := calendarDaysDiff(t, now)
	switch days {
	case 0:
		return "today"
	case 1:
		return "yesterday"
	case 2:
		return "the day before yesterday"
	case -1:
		return "tomorrow"
	case -2:
		return "the day after tomorrow"
	}
	if days > 2 && days < 7 {
		return fmt.Sprintf("%d days ago", days)
	}
	if days < -2 && days > -7 {
		return fmt.Sprintf("in %d days", -days)
	}
	weeks := days / 7
	if weeks == 1 {
		return "1 week ago"
	}
	if weeks >= 2 && weeks < 4 {
		return fmt.Sprintf("%d weeks ago", weeks)
	}
	if weeks == -1 {
		return "in 1 week"
	}
	if weeks <= -2 && weeks > -4 {
		return fmt.Sprintf("in %d weeks", -weeks)
	}
	if days >= 28 && days < 365 {
		months := days / 30
		if months < 1 {
			months = 1
		}
		if months == 1 {
			return "1 month ago"
		}
		return fmt.Sprintf("%d months ago", months)
	}
	if days <= -28 && days > -365 {
		months := -days / 30
		if months < 1 {
			months = 1
		}
		if months == 1 {
			return "in 1 month"
		}
		return fmt.Sprintf("in %d months", months)
	}
	if days >= 365 {
		years := days / 365
		if years == 1 {
			return "1 year ago"
		}
		return fmt.Sprintf("%d years ago", years)
	}
	if days <= -365 {
		years := -days / 365
		if years == 1 {
			return "in 1 year"
		}
		return fmt.Sprintf("in %d years", years)
	}
	return t.Format("2006-01-02")
}
