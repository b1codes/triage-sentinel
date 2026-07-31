package version

import "testing"

func TestGet(t *testing.T) {
	tests := []struct {
		name string
		set  string
		want string
	}{
		{name: "unset returns dev", set: "", want: "dev"},
		{name: "set returns value", set: "1.2.3", want: "1.2.3"},
		{name: "whitespace only returns dev", set: "   ", want: "dev"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := Version
			t.Cleanup(func() { Version = original })

			Version = tc.set
			if got := Get(); got != tc.want {
				t.Errorf("Get() = %q, want %q", got, tc.want)
			}
		})
	}
}
