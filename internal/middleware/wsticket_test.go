package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWSTicketer(t *testing.T) {
	if _, err := NewWSTicketer("short", time.Second); err == nil {
		t.Fatal("secreto corto aceptado")
	}
	tk, err := NewWSTicketer("0123456789abcdef0123456789abcdef", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tk.now = func() time.Time { return now }

	ticket, _ := tk.Issue("acme")
	if tenant, err := tk.Verify(ticket); err != nil || tenant != "acme" {
		t.Fatalf("verify = %q, %v", tenant, err)
	}

	payload, sig, _ := strings.Cut(ticket, ".")
	other, _ := tk.Issue("evil")
	forged := strings.SplitN(other, ".", 2)[0] + "." + sig
	if _, err := tk.Verify(forged); err == nil {
		t.Fatal("ticket manipulado aceptado")
	}
	_ = payload

	tk.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err := tk.Verify(ticket); err == nil {
		t.Fatal("ticket caducado aceptado")
	}
}

func TestWSAuthModesAndIssueHandler(t *testing.T) {
	tk, _ := NewWSTicketer("0123456789abcdef0123456789abcdef", time.Minute)
	auth, _ := NewTenantAuth("admin", map[string]string{"key-a": "tenant-a"})
	var seen string
	next := func(w http.ResponseWriter, r *http.Request) { seen = WSTenantFromContext(r.Context()) }

	issue := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/ws-ticket", nil)
	req.Header.Set(HeaderAPIKey, "key-a")
	auth.Read(tk.IssueHandler())(issue, req)
	if issue.Code != http.StatusOK {
		t.Fatalf("issue status %d", issue.Code)
	}
	ticket, _ := tk.Issue("tenant-a")

	for _, tc := range []struct {
		mode, query string
		want        int
	}{
		{WSModeTicket, "ticket=" + ticket, 200},
		{WSModeTicket, "token=admin", 401},
		{WSModeTicket, "ticket=bad.bad", 401},
		{WSModeToken, "token=admin", 200},
		{WSModeAny, "token=admin", 200},
	} {
		seen = "unset"
		rec := httptest.NewRecorder()
		WSAuth(tc.mode, tk, "admin")(next)(rec, httptest.NewRequest(http.MethodGet, "/ws?"+tc.query, nil))
		if rec.Code != tc.want {
			t.Errorf("%s %s: status %d, want %d", tc.mode, tc.query, rec.Code, tc.want)
		}
		if tc.want == 200 && strings.HasPrefix(tc.query, "ticket=") && seen != "tenant-a" {
			t.Errorf("tenant del contexto = %q", seen)
		}
	}
}
