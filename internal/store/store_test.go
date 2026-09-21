package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func eventJSON(source, id, ts, extra string) []byte {
	return []byte(fmt.Sprintf(`{"source":%q,"event_id":%q,"timestamp":%q,"extra":%s}`,
		source, id, ts, extra))
}

func mustIngest(t *testing.T, st *Store, raw []byte, inc string) IngestResult {
	t.Helper()
	res, err := st.IngestEvent(raw, inc)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return res
}

func mustCreateIncident(t *testing.T, st *Store) string {
	t.Helper()
	inc, err := st.CreateIncident("t")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	return inc.ID
}

// 1. 重复投递：同一 source+event_id 只保留一份原始事实，
// 但接收次数累计、最近接收时间更新，原始字节不变。
func TestDuplicateDelivery(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)

	first := eventJSON("app", "e1", "2026-09-21T10:00:00Z", `"first payload body"`)
	second := eventJSON("app", "e1", "2026-09-21T10:05:00Z", `"different redelivery body"`)

	r1 := mustIngest(t, st, first, inc)
	r2 := mustIngest(t, st, second, inc)

	if r1.ReceiveCount != 1 || r1.Duplicate {
		t.Fatalf("first ingest: %+v", r1)
	}
	if r2.ReceiveCount != 2 || !r2.Duplicate {
		t.Fatalf("duplicate ingest: %+v", r2)
	}
	ev, err := st.GetEvent("app/e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.events) != 1 {
		t.Fatalf("expected 1 deduplicated event, got %d", len(st.events))
	}
	if !bytes.Equal(ev.Raw, first) {
		t.Fatalf("raw fact was rewritten:\nwant %s\n got %s", first, ev.Raw)
	}
	if len(ev.Receptions) != 2 {
		t.Fatalf("want 2 receptions, got %d", len(ev.Receptions))
	}
	if !ev.Receptions[1].At.After(ev.Receptions[0].At) &&
		!ev.Receptions[1].At.Equal(ev.Receptions[0].At) {
		t.Fatal("last reception time did not advance")
	}
	inc2, _ := st.incidentOf("app/e1")
	if inc2 == nil || len(inc2.Members) != 1 {
		t.Fatal("duplicate delivery must not double-add membership")
	}
}

// 2. 迟到排序：后收到的旧事件仍按事件时间插入正确位置；
// 相同时间戳靠首次接收序号稳定排序。
func TestLateArrivalAndTimestampTies(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)

	mustIngest(t, st, eventJSON("db", "late", "2026-09-21T09:00:00Z", `1`), "")
	mustIngest(t, st, eventJSON("app", "later", "2026-09-21T12:00:00Z", `2`), inc)
	mustIngest(t, st, eventJSON("app", "early", "2026-09-21T10:00:00Z", `3`), inc)
	if err := st.AssignEvent(inc, "db/late"); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, st, eventJSON("app", "tie-a", "2026-09-21T10:00:00Z", `4`), inc)
	mustIngest(t, st, eventJSON("app", "tie-b", "2026-09-21T10:00:00Z", `5`), inc)

	view, err := st.ViewIncident(inc, false)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range view.Timeline {
		keys = append(keys, e.Key)
	}
	want := []string{"db/late", "app/early", "app/tie-a", "app/tie-b", "app/later"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("timeline order = %v, want %v", keys, want)
	}
	var recv []string
	for _, e := range view.Reception {
		recv = append(recv, e.Key)
	}
	wantRecv := []string{"db/late", "app/later", "app/early", "app/tie-a", "app/tie-b"}
	if fmt.Sprint(recv) != fmt.Sprint(wantRecv) {
		t.Fatalf("reception order = %v, want %v", recv, wantRecv)
	}
}

// 3. 成环拒绝：返回可读的环路路径；合法边正常建立。
func TestCycleRejection(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)
	for _, k := range []string{"a", "b", "c", "d"} {
		mustIngest(t, st, eventJSON("app", k, "2026-09-21T10:00:00Z", `"x"`), inc)
	}
	decl := func(from, to string) error {
		_, err := st.DeclareEdge(inc, "app/"+from, "app/"+to, EdgeBefore, "")
		return err
	}
	if err := decl("a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := decl("b", "c"); err != nil {
		t.Fatal(err)
	}
	err := decl("c", "a")
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("want CycleError, got %v", err)
	}
	wantPath := "app/c -> app/a -> app/b -> app/c"
	if err.Error() != "edge would create a cycle: "+wantPath {
		t.Fatalf("cycle message = %q", err.Error())
	}
	// 非环边仍然允许。
	if err := decl("c", "d"); err != nil {
		t.Fatalf("non-cycle edge rejected: %v", err)
	}
	// 自环拒绝。
	if err := decl("a", "a"); err == nil {
		t.Fatal("self-loop accepted")
	}
}

// 4. 部分批量失败：坏行逐行报错，好行照样入库。
func TestBulkPartialFailure(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)
	rows := [][]byte{
		eventJSON("app", "ok1", "2026-09-21T10:00:00Z", `1`),
		[]byte(`{not json`),
		[]byte(`{"source":"app","event_id":"","timestamp":"2026-09-21T10:00:00Z"}`),
		[]byte(`{"source":"app","event_id":"badts","timestamp":"not-a-time"}`),
		eventJSON("app", "ok2", "2026-09-21T10:01:00Z", `2`),
	}
	results := st.BulkIngest(rows, inc)
	accepted, failed := 0, 0
	for _, r := range results {
		if r.Accepted {
			accepted++
		} else {
			failed++
			if r.Error == "" {
				t.Fatalf("row %d failed without error message", r.Index)
			}
		}
	}
	if accepted != 2 || failed != 3 {
		t.Fatalf("accepted=%d failed=%d, results=%+v", accepted, failed, results)
	}
	if !results[1].Accepted && results[1].Index != 1 {
		t.Fatal("per-row index not preserved")
	}
	view, _ := st.ViewIncident(inc, false)
	if len(view.Reception) != 2 {
		t.Fatalf("want 2 ingested members, got %d", len(view.Reception))
	}
}

// 5. 并发版本冲突：同一 base 版本的并发校正只有一个成功，其余 409。
func TestConcurrentCorrectionConflict(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)
	mustIngest(t, st, eventJSON("app", "e", "2026-09-21T10:00:00Z", `1`), inc)

	const n = 8
	var wg sync.WaitGroup
	okC, conflictC := 0, 0
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ts := fmt.Sprintf("2026-09-21T11:%02d:00Z", i)
			_, err := st.CorrectEvent("app/e", ts, "race", 0)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				okC++
			} else {
				var ce *ConflictError
				if errors.As(err, &ce) {
					conflictC++
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if okC != 1 || conflictC != n-1 {
		t.Fatalf("ok=%d conflict=%d", okC, conflictC)
	}
	ev, _ := st.GetEvent("app/e")
	if len(ev.Versions) != 2 {
		t.Fatalf("want 2 versions (0 and 1), got %d", len(ev.Versions))
	}
	// 基于旧版本再次提交仍然冲突；基于新版本提交成功。
	if _, err := st.CorrectEvent("app/e", "2026-09-21T12:00:00Z", "stale", 0); err == nil {
		t.Fatal("stale base version after correction must conflict")
	}
	if _, err := st.CorrectEvent("app/e", "2026-09-21T12:00:00Z", "fresh", 1); err != nil {
		t.Fatalf("correction on current version failed: %v", err)
	}
}

// 6. 崩溃后恢复：重新打开后所有状态从审计流还原。
func TestCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	incObj, err := st.CreateIncident("支付中断")
	if err != nil {
		t.Fatal(err)
	}
	inc := incObj.ID
	mustIngest(t, st, eventJSON("app", "e1", "2026-09-21T10:00:00Z", `"a"`), inc)
	mustIngest(t, st, eventJSON("app", "e1", "2026-09-21T10:00:00Z", `"a2"`), inc)
	mustIngest(t, st, eventJSON("db", "e2", "2026-09-21T09:00:00Z", `"b"`), inc)
	if _, err := st.CorrectEvent("db/e2", "2026-09-21T09:30:00Z", "时钟漂移", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeclareEdge(inc, "db/e2", "app/e1", EdgeCauses, "慢查询拖垮应用"); err != nil {
		t.Fatal(err)
	}
	beforeSeq := st.NextSeq()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟杀掉进程后重启。
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if st2.NextSeq() != beforeSeq {
		t.Fatalf("seq after reopen = %d, want %d", st2.NextSeq(), beforeSeq)
	}
	ev, err := st2.GetEvent("app/e1")
	if err != nil {
		t.Fatal(err)
	}
	if ev.ReceiveCount != 2 || len(ev.Receptions) != 2 {
		t.Fatalf("reception history lost: %+v", ev)
	}
	if !bytes.Equal(ev.Raw, eventJSON("app", "e1", "2026-09-21T10:00:00Z", `"a"`)) {
		t.Fatal("original raw bytes changed after recovery")
	}
	view, err := st2.ViewIncident(inc, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Timeline) != 2 || view.Timeline[0].Key != "db/e2" {
		t.Fatalf("corrected timeline wrong after recovery: %+v", view.Timeline)
	}
	if len(view.Edges) != 1 || view.Edges[0].Rationale != "慢查询拖垮应用" {
		t.Fatalf("edges lost after recovery: %+v", view.Edges)
	}
	// 审计文件确实存在于项目数据目录内。
	if _, err := os.Stat(filepath.Join(dir, "audit.log")); err != nil {
		t.Fatal(err)
	}
}
