package memory

import (
	"context"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// fixedAnchor is a deterministic anchor for tests.
var fixedAnchor = time.Date(2026, 4, 21, 0, 0, 0, 0, time.UTC)
var fixedNow = time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC)

// ----------------------------------------------------------------------
// Chinese relative time
// ----------------------------------------------------------------------

func TestResolveInContent_CN_Yesterday(t *testing.T) {
	content, meta := resolveInContent("用户昨天遇到了一个 bug", fixedAnchor, "header")
	want := "用户2026-04-20遇到了一个 bug"
	if content != want {
		t.Errorf("content: want %q, got %q", want, content)
	}
	if meta == nil {
		t.Fatal("expected metadata")
	}
	if meta.ResolvedStart != "2026-04-20" {
		t.Errorf("resolved: want 2026-04-20, got %s", meta.ResolvedStart)
	}
	if meta.Display != "昨天" {
		t.Errorf("display: want 昨天, got %s", meta.Display)
	}
	if meta.AnchorSource != "header" {
		t.Errorf("anchor source: want header, got %s", meta.AnchorSource)
	}
	if meta.Kind != "header_anchor_relative" {
		t.Errorf("kind: want header_anchor_relative, got %s", meta.Kind)
	}
}

func TestResolveInContent_CN_TodayTomorrow(t *testing.T) {
	tests := []struct {
		input    string
		want     string
		resolved string
		display  string
	}{
		{"今天发布", "2026-04-21发布", "2026-04-21", "今天"},
		{"明天上线", "2026-04-22上线", "2026-04-22", "明天"},
		{"前天崩溃", "2026-04-19崩溃", "2026-04-19", "前天"},
		{"后天评审", "2026-04-23评审", "2026-04-23", "后天"},
	}
	for _, tt := range tests {
		content, meta := resolveInContent(tt.input, fixedAnchor, "now")
		if content != tt.want {
			t.Errorf("%q: want %q, got %q", tt.input, tt.want, content)
		}
		if meta == nil || meta.ResolvedStart != tt.resolved {
			t.Errorf("%q: resolved want %s, got %v", tt.input, tt.resolved, meta)
		}
		if meta.Display != tt.display {
			t.Errorf("%q: display want %s, got %s", tt.input, tt.display, meta.Display)
		}
	}
}

func TestResolveInContent_CN_WeekMonthYear(t *testing.T) {
	tests := []struct {
		input   string
		wantISO string
		display string
	}{
		{"上周的会议", "2026-04-14", "上周"},
		{"本周计划", "2026-04-21", "本周"},
		{"下周迭代", "2026-04-28", "下周"},
		{"上个月数据", "2026-03-21", "上个月"},
		{"本月目标", "2026-04-21", "本月"},
		{"下个月发布", "2026-05-21", "下个月"},
		{"去年架构", "2025-04-21", "去年"},
		{"今年规划", "2026-04-21", "今年"},
		{"明年目标", "2027-04-21", "明年"},
	}
	for _, tt := range tests {
		_, meta := resolveInContent(tt.input, fixedAnchor, "now")
		if meta == nil {
			t.Fatalf("%q: expected metadata", tt.input)
		}
		if meta.ResolvedStart != tt.wantISO {
			t.Errorf("%q: want %s, got %s", tt.input, tt.wantISO, meta.ResolvedStart)
		}
		if meta.Display != tt.display {
			t.Errorf("%q: display want %s, got %s", tt.input, tt.display, meta.Display)
		}
		if meta.ResolvedEnd == meta.ResolvedStart {
			t.Errorf("%q: expected ResolvedEnd != ResolvedStart for non-day granularity", tt.input)
		}
	}
}

// Multiple temporal expressions in one content.
func TestResolveInContent_MultipleTemps(t *testing.T) {
	content, meta := resolveInContent("上周 meeting，下周 review", fixedAnchor, "now")
	want := "2026-04-14 meeting，2026-04-28 review"
	if content != want {
		t.Errorf("want %q, got %q", want, content)
	}
	// Metadata should reflect the first match in the original content (上周).
	if meta == nil || meta.Display != "上周" {
		t.Errorf("expected metadata for 上周, got %v", meta)
	}
}

// ----------------------------------------------------------------------
// English relative time
// ----------------------------------------------------------------------

func TestResolveInContent_EN_Basic(t *testing.T) {
	tests := []struct {
		input    string
		want     string
		resolved string
		display  string
	}{
		{"deployed yesterday", "deployed 2026-04-20", "2026-04-20", "yesterday"},
		{"deploy today", "deploy 2026-04-21", "2026-04-21", "today"},
		{"release tomorrow", "release 2026-04-22", "2026-04-22", "tomorrow"},
		{"broke the day before yesterday", "broke 2026-04-19", "2026-04-19", "the day before yesterday"},
		{"review the day after tomorrow", "review 2026-04-23", "2026-04-23", "the day after tomorrow"},
	}
	for _, tt := range tests {
		content, meta := resolveInContent(tt.input, fixedAnchor, "now")
		if content != tt.want {
			t.Errorf("%q: want %q, got %q", tt.input, tt.want, content)
		}
		if meta == nil || meta.ResolvedStart != tt.resolved {
			t.Errorf("%q: resolved want %s, got %v", tt.input, tt.resolved, meta)
		}
		if meta.Display != tt.display {
			t.Errorf("%q: display want %s, got %s", tt.input, tt.display, meta.Display)
		}
	}
}

func TestResolveInContent_EN_WeekMonthYear(t *testing.T) {
	tests := []struct {
		input   string
		wantISO string
		display string
	}{
		{"last week meeting", "2026-04-14", "last week"},
		{"this week plan", "2026-04-21", "this week"},
		{"next week sprint", "2026-04-28", "next week"},
		{"last month data", "2026-03-21", "last month"},
		{"this month goal", "2026-04-21", "this month"},
		{"next month release", "2026-05-21", "next month"},
		{"last year architecture", "2025-04-21", "last year"},
		{"this year roadmap", "2026-04-21", "this year"},
		{"next year target", "2027-04-21", "next year"},
	}
	for _, tt := range tests {
		_, meta := resolveInContent(tt.input, fixedAnchor, "now")
		if meta == nil {
			t.Fatalf("%q: expected metadata", tt.input)
		}
		if meta.ResolvedStart != tt.wantISO {
			t.Errorf("%q: want %s, got %s", tt.input, tt.wantISO, meta.ResolvedStart)
		}
		if meta.Display != tt.display {
			t.Errorf("%q: display want %s, got %s", tt.input, tt.display, meta.Display)
		}
	}
}

// ----------------------------------------------------------------------
// Absolute dates
// ----------------------------------------------------------------------

func TestResolveInContent_AbsoluteISO(t *testing.T) {
	content, meta := resolveInContent("Meeting on 2026-04-15", fixedAnchor, "now")
	if content != "Meeting on 2026-04-15" {
		t.Errorf("content should be unchanged, got %q", content)
	}
	if meta == nil {
		t.Fatal("expected metadata for absolute date")
	}
	if meta.Kind != "explicit_absolute" {
		t.Errorf("kind want explicit_absolute, got %s", meta.Kind)
	}
	if meta.ResolvedStart != "2026-04-15" {
		t.Errorf("resolved want 2026-04-15, got %s", meta.ResolvedStart)
	}
}

func TestResolveInContent_AbsoluteCN(t *testing.T) {
	content, meta := resolveInContent("会议在2026年4月15日举行", fixedAnchor, "now")
	if content != "会议在2026年4月15日举行" {
		t.Errorf("content should be unchanged, got %q", content)
	}
	if meta == nil || meta.ResolvedStart != "2026-04-15" {
		t.Fatalf("expected metadata with 2026-04-15, got %v", meta)
	}
}

func TestResolveInContent_AbsoluteCN_NoYear(t *testing.T) {
	content, meta := resolveInContent("会议在4月15日举行", fixedAnchor, "now")
	if content != "会议在4月15日举行" {
		t.Errorf("content should be unchanged, got %q", content)
	}
	if meta == nil || meta.ResolvedStart != "2026-04-15" {
		t.Fatalf("expected metadata with 2026-04-15, got %v", meta)
	}
}

func TestResolveInContent_AbsoluteEN(t *testing.T) {
	content, meta := resolveInContent("Meeting on April 15, 2026", fixedAnchor, "now")
	if content != "Meeting on April 15, 2026" {
		t.Errorf("content should be unchanged, got %q", content)
	}
	if meta == nil || meta.ResolvedStart != "2026-04-15" {
		t.Fatalf("expected metadata with 2026-04-15, got %v", meta)
	}
}

// ----------------------------------------------------------------------
// No temporal expression
// ----------------------------------------------------------------------

func TestResolveInContent_NoTemporal(t *testing.T) {
	content, meta := resolveInContent("This is a plain fact without dates.", fixedAnchor, "now")
	if content != "This is a plain fact without dates." {
		t.Errorf("content changed unexpectedly: %q", content)
	}
	if meta != nil {
		t.Errorf("expected nil metadata, got %v", meta)
	}
}

// ----------------------------------------------------------------------
// NormalizeTemporalFacts
// ----------------------------------------------------------------------

func TestNormalizeTemporalFacts(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: "hello", Timestamp: fixedAnchor},
	}
	facts := []ExtractedFact{
		{Content: "昨天遇到 bug", SourceMsg: 0},
		{Content: "plain fact", SourceMsg: 0},
	}

	out := NormalizeTemporalFacts(facts, messages, fixedAnchor)
	if len(out) != 2 {
		t.Fatalf("expected 2 facts, got %d", len(out))
	}

	if out[0].Content != "2026-04-20遇到 bug" {
		t.Errorf("fact 0: want %q, got %q", "2026-04-20遇到 bug", out[0].Content)
	}
	if out[0].Metadata == nil {
		t.Error("fact 0: expected metadata")
	}

	if out[1].Content != "plain fact" {
		t.Errorf("fact 1: want %q, got %q", "plain fact", out[1].Content)
	}
	if out[1].Metadata != nil {
		t.Errorf("fact 1: expected nil metadata, got %v", out[1].Metadata)
	}
}

func TestNormalizeTemporalFacts_MetadataMerge(t *testing.T) {
	messages := []Message{{Role: "user", Content: "x", Timestamp: fixedAnchor}}
	facts := []ExtractedFact{
		{Content: "明天开会", SourceMsg: 0, Metadata: map[string]interface{}{"source": "slack"}},
	}
	out := NormalizeTemporalFacts(facts, messages, fixedAnchor)
	if out[0].Metadata == nil {
		t.Fatal("expected metadata")
	}
	if out[0].Metadata["source"] != "slack" {
		t.Errorf("existing metadata lost")
	}
	if _, ok := out[0].Metadata["temporal"]; !ok {
		t.Errorf("temporal metadata not injected")
	}
}

// ----------------------------------------------------------------------
// NormalizeTemporalRecallQuery
// ----------------------------------------------------------------------

func TestNormalizeTemporalRecallQuery(t *testing.T) {
	q := NormalizeTemporalRecallQuery("昨天的问题", fixedAnchor)
	if q != "2026-04-20的问题" {
		t.Errorf("want %q, got %q", "2026-04-20的问题", q)
	}

	q2 := NormalizeTemporalRecallQuery("issues from last week", fixedAnchor)
	if q2 != "issues from 2026-04-14" {
		t.Errorf("want %q, got %q", "issues from 2026-04-14", q2)
	}
}

// ----------------------------------------------------------------------
// TemporalRecallProjection
// ----------------------------------------------------------------------

func TestTemporalRecallProjection_WithMetadata(t *testing.T) {
	meta := map[string]interface{}{
		"temporal": &TemporalMetadata{
			ResolvedStart: "2026-04-20",
			Display:       "昨天",
		},
	}
	out := TemporalRecallProjection("在 2026-04-20 遇到 bug", meta, fixedNow)
	if out != "在 昨天 遇到 bug" {
		t.Errorf("want %q, got %q", "在 昨天 遇到 bug", out)
	}
}

// Test projection after JSON round-trip (map[string]interface{} path).
func TestTemporalRecallProjection_Deserialized(t *testing.T) {
	meta := map[string]interface{}{
		"temporal": map[string]interface{}{
			"resolved_start": "2026-04-20",
			"display":        "昨天",
		},
	}
	out := TemporalRecallProjection("在 2026-04-20 遇到 bug", meta, fixedNow)
	if out != "在 昨天 遇到 bug" {
		t.Errorf("want %q, got %q", "在 昨天 遇到 bug", out)
	}
}

func TestTemporalRecallProjection_WithoutMetadata(t *testing.T) {
	out := TemporalRecallProjection("Meeting on 2026-04-21", nil, fixedNow)
	if out != "Meeting on yesterday" {
		t.Errorf("want %q, got %q", "Meeting on yesterday", out)
	}
}

// ----------------------------------------------------------------------
// Anchor extraction
// ----------------------------------------------------------------------

func TestExtractAnchorDate(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	msgs := []Message{
		{Role: "user", Content: "x", Timestamp: fixedAnchor},
	}
	got, src := extractAnchorDate(msgs, now)
	if !got.Equal(fixedAnchor) {
		t.Errorf("want %v, got %v", fixedAnchor, got)
	}
	if src != "header" {
		t.Errorf("want header, got %s", src)
	}

	got2, src2 := extractAnchorDate(nil, now)
	if !got2.Equal(now) {
		t.Errorf("want %v, got %v", now, got2)
	}
	if src2 != "now" {
		t.Errorf("want now, got %s", src2)
	}
}

// ----------------------------------------------------------------------
// Edge cases
// ----------------------------------------------------------------------

func TestResolveInContent_InvalidDate(t *testing.T) {
	content, meta := resolveInContent("Event on 2026-02-30", fixedAnchor, "now")
	if meta != nil {
		t.Errorf("invalid date should not produce metadata, got %v", meta)
	}
	if content != "Event on 2026-02-30" {
		t.Errorf("content should be unchanged, got %q", content)
	}
}

func TestResolveInContent_YearBoundary(t *testing.T) {
	anchor := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	content, meta := resolveInContent("明天新年", anchor, "now")
	if meta == nil || meta.ResolvedStart != "2027-01-01" {
		t.Errorf("year boundary: want 2027-01-01, got %v", meta)
	}
	if content != "2027-01-01新年" {
		t.Errorf("content: want %q, got %q", "2027-01-01新年", content)
	}
}

func TestHumanRelative(t *testing.T) {
	tests := []struct {
		t    time.Time
		want string
	}{
		{time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC), "today"},
		{time.Date(2026, 4, 21, 0, 0, 0, 0, time.UTC), "yesterday"},
		{time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC), "the day before yesterday"},
		{time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC), "tomorrow"},
		{time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC), "the day after tomorrow"},
		{time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC), "4 days ago"},
		{time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC), "1 week ago"},
		{time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC), "2 weeks ago"},
	}
	for _, tt := range tests {
		got := humanRelative(tt.t, fixedNow)
		if got != tt.want {
			t.Errorf("humanRelative(%s): want %q, got %q", tt.t.Format("2006-01-02"), tt.want, got)
		}
	}
}

// ----------------------------------------------------------------------
// Review fixes — additional tests
// ----------------------------------------------------------------------

// P0-1: multiple adjacent Chinese temporal expressions.
func TestResolveInContent_MultipleCNAdjacent(t *testing.T) {
	content, meta := resolveInContent("去年今天", fixedAnchor, "now")
	// Both should be replaced independently.
	want := "2025-04-212026-04-21"
	if content != want {
		t.Errorf("want %q, got %q", want, content)
	}
	if meta == nil || meta.Display != "去年" {
		t.Errorf("expected metadata for 去年, got %v", meta)
	}
}

// P0-3: cross-year month-day (anchor late in year).
func TestResolveInContent_CrossYear_EarlyYear(t *testing.T) {
	anchor := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	_, meta := resolveInContent("会议在1月15日举行", anchor, "now")
	if meta == nil || meta.ResolvedStart != "2027-01-15" {
		t.Errorf("cross-year: want 2027-01-15, got %v", meta)
	}
}

// P0-3: cross-year month-day (anchor early in year).
func TestResolveInContent_CrossYear_LateYear(t *testing.T) {
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, meta := resolveInContent("会议在12月15日举行", anchor, "now")
	if meta == nil || meta.ResolvedStart != "2025-12-15" {
		t.Errorf("cross-year: want 2025-12-15, got %v", meta)
	}
}

// P1-3: Chinese substring boundary check.
func TestResolveInContent_CN_Boundary(t *testing.T) {
	content, meta := resolveInContent("今年轻人喜欢编程", fixedAnchor, "now")
	if meta != nil {
		t.Errorf("substring match should be blocked, got content=%q meta=%v", content, meta)
	}
	if content != "今年轻人喜欢编程" {
		t.Errorf("content should be unchanged, got %q", content)
	}
}

// P2-5: season patterns.
func TestResolveInContent_CN_Season(t *testing.T) {
	content, meta := resolveInContent("上季度的报告", fixedAnchor, "now")
	want := "2026-01-21的报告"
	if content != want {
		t.Errorf("want %q, got %q", want, content)
	}
	if meta == nil || meta.Granularity != "season" {
		t.Errorf("want granularity=season, got %v", meta)
	}
}

func TestResolveInContent_EN_Season(t *testing.T) {
	content, meta := resolveInContent("review last season", fixedAnchor, "now")
	want := "review 2026-01-21"
	if content != want {
		t.Errorf("want %q, got %q", want, content)
	}
	if meta == nil || meta.Granularity != "season" {
		t.Errorf("want granularity=season, got %v", meta)
	}
}

// P0-4: humanRelative with time-of-day component.
func TestHumanRelative_TimeComponent(t *testing.T) {
	now := time.Date(2026, 4, 22, 1, 0, 0, 0, time.UTC)
	t20 := time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC)
	got := humanRelative(t20, now)
	// Calendar-day diff is 2, not 1 (int(26h/24h)).
	if got != "the day before yesterday" {
		t.Errorf("time-component: want 'the day before yesterday', got %q", got)
	}
}

// P2-4: humanRelative month / year descriptions.
func TestHumanRelative_MonthsYears(t *testing.T) {
	now := time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		t    time.Time
		want string
	}{
		{time.Date(2026, 3, 22, 0, 0, 0, 0, time.UTC), "1 month ago"},
		{time.Date(2026, 1, 22, 0, 0, 0, 0, time.UTC), "3 months ago"},
		{time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC), "1 year ago"},
		{time.Date(2024, 4, 22, 0, 0, 0, 0, time.UTC), "2 years ago"},
		{time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC), "in 1 month"},
		{time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC), "in 3 months"},
		{time.Date(2027, 4, 22, 0, 0, 0, 0, time.UTC), "in 1 year"},
		{time.Date(2028, 4, 22, 0, 0, 0, 0, time.UTC), "in 2 years"},
	}
	for _, tt := range tests {
		got := humanRelative(tt.t, now)
		if got != tt.want {
			t.Errorf("humanRelative(%s): want %q, got %q", tt.t.Format("2006-01-02"), tt.want, got)
		}
	}
}

// P0-2: TemporalRecallProjection applied in toMemories.
func TestToMemories_TemporalProjection(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	mem := &Memory{
		ID:      "test-proj-id",
		Content: "在 2026-04-20 遇到 bug",
		State:   StateActive,
		Metadata: map[string]interface{}{
			"temporal": &TemporalMetadata{
				ResolvedStart: "2026-04-20",
				Display:       "昨天",
			},
		},
	}
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 0.1
	}
	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		t.Fatalf("memoryToDoc: %v", err)
	}

	results := []vego.SearchResult{{Document: doc}}
	out, err := s.toMemories(results)
	if err != nil {
		t.Fatalf("toMemories: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 result, got %d", len(out))
	}
	if out[0].Content != "在 昨天 遇到 bug" {
		t.Errorf("projection: want %q, got %q", "在 昨天 遇到 bug", out[0].Content)
	}
}

// ----------------------------------------------------------------------
// Additional review fixes
// ----------------------------------------------------------------------

// P1: StoreRawMessages dedup uses original content, not normalized content.
func TestStoreRawMessages_CrossDayDedup(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	sessionID := "sess-1"

	// Day 1: store "昨天发布"
	day1 := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	messages := []Message{
		{Role: "user", Content: "昨天发布", Timestamp: day1},
	}
	stored, err := s.StoreRawMessages(ctx, sessionID, messages)
	if err != nil {
		t.Fatalf("day1 store: %v", err)
	}
	if stored != 1 {
		t.Fatalf("day1: want 1 stored, got %d", stored)
	}

	// Day 2: same original message, different anchor date.
	day2 := time.Date(2026, 4, 21, 0, 0, 0, 0, time.UTC)
	messages[0].Timestamp = day2
	stored, err = s.StoreRawMessages(ctx, sessionID, messages)
	if err != nil {
		t.Fatalf("day2 store: %v", err)
	}
	if stored != 0 {
		t.Errorf("P1: cross-day dedup failed, want 0 stored, got %d", stored)
	}
}

// P2: English case-insensitive matching.
func TestResolveInContent_EN_CaseInsensitive(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		display string
	}{
		{"deployed Yesterday", "deployed 2026-04-20", "yesterday"},
		{"release TOMORROW", "release 2026-04-22", "tomorrow"},
		{"meeting LAST WEEK", "meeting 2026-04-14", "last week"},
		{"plan This Month", "plan 2026-04-21", "this month"},
	}
	for _, tt := range tests {
		content, meta := resolveInContent(tt.input, fixedAnchor, "now")
		if content != tt.want {
			t.Errorf("%q: want %q, got %q", tt.input, tt.want, content)
		}
		if meta == nil || meta.Display != tt.display {
			t.Errorf("%q: display want %s, got %v", tt.input, tt.display, meta)
		}
	}
}

// P2-1: Chinese false-positive "上个月饼".
func TestResolveInContent_CN_FalsePositive_Mooncake(t *testing.T) {
	content, meta := resolveInContent("上个月饼很好吃", fixedAnchor, "now")
	if meta != nil {
		t.Errorf("false positive: '上个月' in '上个月饼' should not match, got content=%q meta=%v", content, meta)
	}
	if content != "上个月饼很好吃" {
		t.Errorf("content should be unchanged, got %q", content)
	}
}
