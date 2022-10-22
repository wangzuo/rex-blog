package db

import (
	"context"
	"database/sql"
	"log"
	"time"
)

var TablePost *Table
var ColumnPostID *IntColumn
var ColumnPostTitle *StringColumn
var ColumnPostContent *StringColumn
var ColumnPostCreatedAt *TimeColumn
var ColumnPostUpdatedAt *TimeColumn

func init() {
	TablePost = NewTable("posts")
	ColumnPostID = TablePost.NewIntColumn("id")
	ColumnPostTitle = TablePost.NewStringColumn("title")
	ColumnPostContent = TablePost.NewStringColumn("content")
	ColumnPostCreatedAt = TablePost.NewTimeColumn("created_at")
	ColumnPostUpdatedAt = TablePost.NewTimeColumn("updated_at")
}

type PostClient struct {
	adapter *Adapter
}

type PostQuery struct {
	context         context.Context
	client          *PostClient
	selectStatement *SelectStatement
}

type Post struct {
	ID        int            `json:"id"`
	Title     sql.NullString `json:"title"`
	Content   sql.NullString `json:"content"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

type PostCreateInput struct {
	ID        int
	Title     *string
	Content   *string
	CreatedAt *time.Time
	UpdatedAt *time.Time
}

type PostUpdateInput struct {
	ID        int
	Title     *string
	Content   *string
	CreatedAt *time.Time
	UpdatedAt *time.Time
}

func (c *PostClient) Query() *PostQuery {
	sel := &SelectStatement{}
	sel.Select("posts.*")
	sel.From("posts")

	return &PostQuery{
		selectStatement: sel,
		client:          c,
	}
}

func (c *PostClient) QueryContext(ctx context.Context) *PostQuery {
	sel := &SelectStatement{}
	sel.Select("posts.*")
	sel.From("posts")

	return &PostQuery{
		selectStatement: sel,
		context:         ctx,
		client:          c,
	}
}

// TODO: code share with q.All()
func (c *PostClient) RawQueryContext(ctx context.Context, sqlStr string, args ...interface{}) ([]*Post, error) {
	var rows *sql.Rows
	var err error

	if ctx != nil {
		rows, err = c.adapter.QueryContext(ctx, sqlStr, args...)
	} else {
		rows, err = c.adapter.Query(sqlStr, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	var posts []*Post

	for rows.Next() {
		var post Post
		// TODO: structscan
		for i := range columns {
			switch columns[i] {
			case "id": // TODO: use const for column string
				values[i] = &post.ID
			case "title": // TODO: use const for column string
				values[i] = &post.Title
			case "content": // TODO: use const for column string
				values[i] = &post.Content
			case "created_at": // TODO: use const for column string
				values[i] = &post.CreatedAt
			case "updated_at": // TODO: use const for column string
				values[i] = &post.UpdatedAt
			}
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		posts = append(posts, &post)
	}

	return posts, nil
}

func (c *PostClient) RawQuery(sqlStr string, args ...interface{}) ([]*Post, error) {
	return c.RawQueryContext(nil, sqlStr, args...)
}

func (c *PostClient) Create(input *PostCreateInput) (*Post, error) {
	return c.CreateContext(nil, input)
}

func (c *PostClient) CreateContext(ctx context.Context, input *PostCreateInput) (*Post, error) {
	now := time.Now()
	input.CreatedAt = &now
	input.UpdatedAt = &now

	s := &InsertStatement{}
	s.Into("posts").
		Columns("title", "content", "created_at", "updated_at").
		Values(input.Title, input.Content, input.CreatedAt, input.UpdatedAt).
		Returning("id,title,content,created_at,updated_at")
	sqlStr, args := s.ToSQL()
	log.Println(sqlStr, args)

	tx, err := c.adapter.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	post := &Post{}
	var row *sql.Row
	if ctx != nil {
		row = tx.QueryRowContext(ctx, sqlStr, args...)
	} else {
		row = tx.QueryRow(sqlStr, args...)
	}

	if err := row.Scan(&post.ID, &post.Title, &post.Content, &post.CreatedAt, &post.UpdatedAt); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return post, nil
}

func (q *PostQuery) Where(expr *Clause) *PostQuery {
	q.selectStatement.Where(expr)
	return q
}

func (q *PostQuery) Order() *PostQuery {
	return q
}

func (q *PostQuery) Limit(limit int) *PostQuery {
	q.selectStatement.Limit(limit)
	return q
}

func (q *PostQuery) Offset(limit int) *PostQuery {
	q.selectStatement.Offset(limit)
	return q
}

func (q *PostQuery) All() ([]*Post, error) {
	sqlStr, args := q.selectStatement.ToSQL()
	// TODO: add logger
	// TODO: move logger to adapter
	log.Println(sqlStr, args)

	var rows *sql.Rows
	var err error

	if q.context != nil {
		rows, err = q.client.adapter.QueryContext(q.context, sqlStr, args...)
	} else {
		rows, err = q.client.adapter.Query(sqlStr, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))

	var posts []*Post

	for rows.Next() {
		var post Post
		// TODO: structscan
		for i := range columns {
			switch columns[i] {
			case "id": // TODO: use const for column string
				values[i] = &post.ID
			case "title": // TODO: use const for column string
				values[i] = &post.Title
			case "content": // TODO: use const for column string
				values[i] = &post.Content
			case "created_at": // TODO: use const for column string
				values[i] = &post.CreatedAt
			case "updated_at": // TODO: use const for column string
				values[i] = &post.UpdatedAt
			}
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		posts = append(posts, &post)
	}

	return posts, nil
}

func (q *PostQuery) UpdateAll(input *PostUpdateInput) (int, error) {
	return 0, nil
}

func (q *PostQuery) DeleteAll(input *PostUpdateInput) (int, error) {
	return 0, nil
}

func (q *PostQuery) First() (*Post, error) {
	q.Limit(1) // TODO: add limit integer to args

	posts, err := q.All()
	if err != nil {
		return nil, err
	}

	if len(posts) > 0 {
		return posts[0], nil
	}

	return nil, nil
}

func (c *PostClient) FindContext(ctx context.Context, id int) (*Post, error) {
	q := c.QueryContext(ctx)
	return q.Where(ColumnPostID.EQ(id)).First()
}

func (c *PostClient) Find(id int) (*Post, error) {
	return c.FindContext(nil, id)
}

func (c *PostClient) UpdateContext(ctx context.Context, input *PostUpdateInput) (*Post, error) {
	var columns []string
	var values []interface{}

	if input.Title != nil {
		columns = append(columns, "title")
		values = append(values, input.Title)
	}

	if input.Content != nil {
		columns = append(columns, "content")
		values = append(values, input.Content)
	}

	columns = append(columns, "updated_at")
	if input.UpdatedAt == nil {
		values = append(values, time.Now())
	} else {
		values = append(values, input.UpdatedAt)
	}

	s := &UpdateStatement{}
	s.Table("posts").Columns(columns...).
		Values(values...).
		Where(ColumnPostID.EQ(input.ID)).
		Returning("id,title,content,created_at,updated_at")
	sqlStr, args := s.ToSQL()
	log.Println(sqlStr, args)

	tx, err := c.adapter.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	post := &Post{}
	var row *sql.Row
	if ctx != nil {
		row = tx.QueryRowContext(ctx, sqlStr, args...)
	} else {
		row = tx.QueryRow(sqlStr, args...)
	}

	if err := row.Scan(&post.ID, &post.Title, &post.Content, &post.CreatedAt, &post.UpdatedAt); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return post, nil
}

func (c *PostClient) Update(input *PostUpdateInput) (*Post, error) {
	return c.UpdateContext(nil, input)
}

func (c *PostClient) InsertAllContext(ctx context.Context, inputs []*PostCreateInput) ([]*Post, error) {
	s := &InsertStatement{}
	s.Into("posts").
		Columns("title", "content", "created_at", "updated_at").
		Values().
		Returning("id,title,content,created_at,updated_at")
	sql, args := s.ToSQL()
	log.Println(sql, args)

	tx, err := c.adapter.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	var items []*Post
	for rows.Next() {
		var post Post
		if err := rows.Scan(&post.ID, &post.Title, &post.Content, &post.CreatedAt, &post.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, &post)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return items, nil
}

func (c *PostClient) InsertAll(inputs []*PostCreateInput) ([]*Post, error) {
	return nil, nil
}

func (u *Post) DeleteContext(ctx context.Context) error {
	return nil
}

func (u *Post) Delete() error {
	return nil
}

func (c *PostClient) Upsert(input []*PostCreateInput) ([]*Post, error) {
	return nil, nil
}

func (c *PostClient) UpsertContext(ctx context.Context, input []*PostCreateInput) ([]*Post, error) {
	return nil, nil
}

func (c *PostClient) UpsertAll(inputs []*PostCreateInput) ([]*Post, error) {
	return nil, nil
}

func (c *PostClient) UpsertAllContext(ctx context.Context, inputs []*PostCreateInput) ([]*Post, error) {
	return nil, nil
}
