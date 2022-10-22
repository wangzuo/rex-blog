package db

import "testing"

func TestInsertStatement(t *testing.T) {
	s := &InsertStatement{}
	s.Into("users").Columns("name", "email").Values("demo", "demo@example.com").Returning("id, name, email")
	t.Log(s.ToSQL())
}

func TestUpdateStatement(t *testing.T) {
	users := NewTable("users")
	userID := users.NewIntColumn("id")
	// userName := users.NewStringColumn("name")
	s := &UpdateStatement{}
	s.Table("users").Columns("name").Values("demo").Where(userID.EQ(1))
	t.Log(s.ToSQL())
}

func TestDeleteStatement(t *testing.T) {
	users := NewTable("users")
	userName := users.NewStringColumn("name")
	s := &DeleteStatemnet{}
	s.From("users").Where(userName.EQ("demo"))
	t.Log(s.ToSQL())
}
