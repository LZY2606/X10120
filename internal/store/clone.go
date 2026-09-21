package store

import "encoding/json"

func cloneIncident(in *Incident) *Incident {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

func cloneEdge(in *Edge) *Edge {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

func cloneEvent(in *Event) *Event {
	if in == nil {
		return nil
	}
	cp := *in
	if in.Raw != nil {
		cp.Raw = append(json.RawMessage(nil), in.Raw...)
	}
	cp.Receipts = append([]Receipt(nil), in.Receipts...)
	cp.Versions = append([]Version(nil), in.Versions...)
	return &cp
}
