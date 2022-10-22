package db

import (
	"database/sql"
	"fmt"
	"os"
)

type Client struct {
	Post *PostClient
}

type Config struct {
	Database string
	User     string
	Password string
	URL      string
}

func NewConfig() *Config {
	env := os.Getenv("REX_ENV")
	if env == "" {
		env = "development"
	}

	switch env {
	case "development":
		return &Config{
			Database: "rex_blog_development",
			User:     "postgres",
			Password: "postgres",
			URL:      "",
		}
	case "production":
		return &Config{
			Database: "",
			User:     "",
			Password: "",
			URL:      "DATABASE_URL",
		}
	}

	return nil
}

func NewClient() (*Client, error) {
	config := NewConfig()
	if config == nil {
		return nil, fmt.Errorf("client config is missing")
	}

	var db *sql.DB
	var err error

	// TODO: fix Getenv
	// TODO: use connection url
	if config.URL != "" {
		db, err = sql.Open("postgres", os.Getenv(config.URL))
	} else {
		db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", config.User, config.Password, config.Database))
	}
	if err != nil {
		return nil, err
	}

	adapter := &Adapter{
		db: db,
	}

	return &Client{
		Post: &PostClient{adapter},
	}, nil
}
