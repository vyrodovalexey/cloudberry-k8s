package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		expected string
	}{
		{
			name: "basic connection string",
			cfg: Config{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "user",
				Password: "pass",
				SSLMode:  "disable",
			},
			expected: "host=localhost port=5432 dbname=testdb user=user password=pass sslmode=disable",
		},
		{
			name: "ssl mode require",
			cfg: Config{
				Host:     "db.example.com",
				Port:     5433,
				Database: "production",
				Username: "admin",
				Password: "secret",
				SSLMode:  "require",
			},
			expected: "host=db.example.com port=5433 dbname=production user=admin password=secret sslmode=require",
		},
		{
			name: "empty ssl mode defaults to disable",
			cfg: Config{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "user",
				Password: "pass",
				SSLMode:  "",
			},
			expected: "host=localhost port=5432 dbname=testdb user=user password=pass sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildConnectionString(tt.cfg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildRoleOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     RoleOptions
		expected string
	}{
		{
			name:     "no options",
			opts:     RoleOptions{Name: "test"},
			expected: "",
		},
		{
			name: "login only",
			opts: RoleOptions{
				Name:  "test",
				Login: true,
			},
			expected: " WITH LOGIN",
		},
		{
			name: "all options",
			opts: RoleOptions{
				Name:       "admin",
				Login:      true,
				SuperUser:  true,
				CreateDB:   true,
				CreateRole: true,
				Password:   "secret",
				ValidUntil: "2025-12-31",
			},
			expected: " WITH LOGIN SUPERUSER CREATEDB CREATEROLE PASSWORD 'secret' VALID UNTIL '2025-12-31'",
		},
		{
			name: "password with quotes",
			opts: RoleOptions{
				Name:     "test",
				Password: "it's a test",
			},
			expected: " WITH PASSWORD 'it''s a test'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildRoleOptions(tt.opts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "hello",
			expected: "'hello'",
		},
		{
			name:     "string with single quote",
			input:    "it's",
			expected: "'it''s'",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "''",
		},
		{
			name:     "multiple quotes",
			input:    "it's a 'test'",
			expected: "'it''s a ''test'''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteLiteral(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEscapeQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no quotes",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "single quote",
			input:    "it's",
			expected: "it''s",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only quotes",
			input:    "'''",
			expected: "''''''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParameterScope(t *testing.T) {
	scope := ParameterScope{Level: "database", Target: "mydb"}
	assert.Equal(t, "database", scope.Level)
	assert.Equal(t, "mydb", scope.Target)
}

func TestSegmentInfo(t *testing.T) {
	seg := SegmentInfo{
		ContentID:     1,
		DBID:          2,
		Role:          "p",
		PreferredRole: "p",
		Mode:          "s",
		Status:        "u",
		Hostname:      "host1",
		Address:       "10.0.0.1",
		Port:          6000,
		DataDirectory: "/data/primary/gpseg1",
	}
	assert.Equal(t, int32(1), seg.ContentID)
	assert.Equal(t, "u", seg.Status)
}

func TestClusterState(t *testing.T) {
	state := ClusterState{
		IsUp:              true,
		Version:           "7.7",
		SegmentsUp:        4,
		SegmentsDown:      0,
		SegmentsTotal:     4,
		MirroringInSync:   true,
		ActiveConnections: 10,
		MaxConnections:    100,
	}
	assert.True(t, state.IsUp)
	assert.True(t, state.MirroringInSync)
}

func TestVacuumOptions(t *testing.T) {
	opts := VacuumOptions{Full: true, Analyze: true, Table: "my_table"}
	assert.True(t, opts.Full)
	assert.True(t, opts.Analyze)
	assert.Equal(t, "my_table", opts.Table)
}

func TestReindexOptions(t *testing.T) {
	opts := ReindexOptions{Database: "mydb", Table: "my_table"}
	assert.Equal(t, "mydb", opts.Database)
	assert.Equal(t, "my_table", opts.Table)
}

func TestDiskUsage(t *testing.T) {
	du := DiskUsage{Database: "mydb", SizeBytes: 1073741824, SizeHuman: "1 GB"}
	assert.Equal(t, "mydb", du.Database)
	assert.Equal(t, int64(1073741824), du.SizeBytes)
}
