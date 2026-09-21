package store

import (
	"os"
	"path/filepath"
	"testing"
)

// 逻辑撤销：被有效边引用时拒绝；删除边后可撤销；撤销后不可再加入新边。
func TestRevokeGuardedByEdges(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)
	mustIngest(t, st, eventJSON("app", "a", "2026-09-21T10:00:00Z", `1`), inc)
	mustIngest(t, st, eventJSON("app", "b", "2026-09-21T10:01:00Z", `2`), inc)
	if _, err := st.DeclareEdge(inc, "app/a", "app/b", EdgeBefore, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RevokeEvent("app/a"); err == nil {
		t.Fatal("revoke must be blocked while an active edge references the event")
	}
	if err := st.RemoveEdge(inc, "app/a", "app/b"); err != nil {
		t.Fatal(err)
	}
	if err := st.RevokeEvent("app/a"); err != nil {
		t.Fatalf("revoke after edge removal: %v", err)
	}
	ev, _ := st.GetEvent("app/a")
	if !ev.Revoked {
		t.Fatal("revoked flag not set")
	}
	// 原始事实仍在。
	if len(ev.Raw) == 0 || ev.ReceiveCount != 1 {
		t.Fatal("revoked event lost its raw fact")
	}
	mustIngest(t, st, eventJSON("app", "c", "2026-09-21T10:02:00Z", `3`), inc)
	if _, err := st.DeclareEdge(inc, "app/c", "app/a", EdgeBefore, ""); err == nil {
		t.Fatal("edge targeting revoked event must be rejected")
	}
}

// 时间校正产生新版本；原始视图保持旧时间，校正视图使用新时间。
func TestCorrectionKeepsRawViewIntact(t *testing.T) {
	st := newTestStore(t)
	inc := mustCreateIncident(t, st)
	mustIngest(t, st, eventJSON("app", "a", "2026-09-21T12:00:00Z", `1`), inc)
	mustIngest(t, st, eventJSON("app", "b", "2026-09-21T11:00:00Z", `2`), inc)

	raw, err := st.ViewIncident(inc, false)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Timeline[0].Key != "app/b" {
		t.Fatal("raw order wrong")
	}
	if _, err := st.CorrectEvent("app/b", "2026-09-21T13:00:00Z", "时钟偏早2小时", 0); err != nil {
		t.Fatal(err)
	}
	raw2, _ := st.ViewIncident(inc, false)
	if raw2.Timeline[0].Key != "app/b" || !raw2.Timeline[0].RawTimestamp.Equal(raw.Timeline[0].RawTimestamp) {
		t.Fatal("raw view was rewritten by a correction")
	}
	corr, _ := st.ViewIncident(inc, true)
	if corr.Timeline[0].Key != "app/a" || corr.Timeline[1].Key != "app/b" {
		t.Fatalf("corrected view order wrong: %s then %s", corr.Timeline[0].Key, corr.Timeline[1].Key)
	}
	if !corr.Timeline[1].Corrected || corr.Timeline[1].Version != 1 {
		t.Fatal("corrected event metadata wrong")
	}
	ev, _ := st.GetEvent("app/b")
	if len(ev.Versions) != 2 || ev.Versions[0].Version != 0 {
		t.Fatalf("old version must remain: %+v", ev.Versions)
	}
}

// 崩溃发生在写入半行时：残行被忽略，此前已提交的记录完整还原。
func TestTornTrailingLineIgnored(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustIngest(t, st, eventJSON("app", "a", "2026-09-21T10:00:00Z", `1`), "")
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "audit.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":99,"type":"event_received","data":{"broken`); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with torn line: %v", err)
	}
	defer st2.Close()
	if _, err := st2.GetEvent("app/a"); err != nil {
		t.Fatal("committed event lost")
	}
	// 序号从已提交的高位继续，不会与残行的 99 冲突。
	if st2.NextSeq() != 1 {
		t.Fatalf("seq = %d, want 1", st2.NextSeq())
	}
	mustIngest(t, st2, eventJSON("app", "b", "2026-09-21T10:01:00Z", `2`), "")
	if st2.NextSeq() != 2 {
		t.Fatalf("seq after new commit = %d, want 2", st2.NextSeq())
	}
}
